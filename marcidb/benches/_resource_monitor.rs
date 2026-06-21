// _resource_monitor.rs
// ─────────────────────────────────────────────────────────────────────────────
// Мониторинг RSS серверных процессов во время Criterion-бенчмарков.
//
// Исправления:
//   [fix-1] Периодическое пересканирование PID-ов внутри цикла сэмплирования
//           (каждые RESCAN_EVERY тиков ≈ 1 с). Новые postgres/marcidb workers,
//           поднятые во время concurrent-бенчмарков, будут подхвачены без
//           перезапуска монитора.
//
//   [fix-2] Пропуск сэмплов в течение warm_up (Duration) с начала start().
//           Criterion по умолчанию прогревает 3 с, поэтому дефолт совпадает.
//           Конструктор with_warm_up() позволяет задать другое значение.
//           В отчёте явно указывается «steady-state», чтобы было понятно,
//           что прогревочные сэмплы не учтены.
// ─────────────────────────────────────────────────────────────────────────────

use criterion::measurement::Measurement;
use criterion::{BenchmarkGroup, Bencher};
use std::sync::{Mutex, atomic::{AtomicBool}};
use std::thread::{self, JoinHandle};
use std::time::{Instant};
use sysinfo::{get_current_pid, Pid, ProcessRefreshKind, RefreshKind, System, ProcessesToUpdate};

const SAMPLE_MS: u64 = 1;

/// Сколько тиков между пересканированиями PID-ов [fix-1].
/// 50 тиков × 20 мс = 1 с.
const RESCAN_EVERY: usize = 50;

// ─── Какой сервер трекаем ─────────────────────────────────────────────────────

pub enum ServerKind {
    Postgres,
    MarciDB,
}

// ─── Статистика одного процесса ───────────────────────────────────────────────

#[derive(Default, Clone)]
pub struct ProcStats {
    pub rss_avg:  f64,
    pub rss_peak: f64,
    pub n:        usize,
}

impl ProcStats {
    fn record(&mut self, rss_bytes: u64) {
        let rss = rss_bytes as f64 / 1_048_576.0;
        self.n       += 1;
        self.rss_avg += rss;
        if rss > self.rss_peak { self.rss_peak = rss; }
    }

    fn finalize(&mut self) {
        if self.n > 0 {
            self.rss_avg /= self.n as f64;
        }
    }
}

#[derive(Default)]
struct Snapshot {
    server: ProcStats,
    bench:  ProcStats,
}

// ─── ResourceMonitor ──────────────────────────────────────────────────────────

pub struct ResourceMonitor {
    server_pids:  Vec<Pid>,
    /// Подстроки имён серверных процессов — нужны для пересканирования [fix-1].
    server_names: Vec<&'static str>,
    bench_pid:    Option<Pid>,
    /// Время, в течение которого сэмплы не записываются (фаза прогрева) [fix-2].
    warm_up:      Duration,
}

impl ResourceMonitor {
    /// Стандартный конструктор: warm_up = 3 с (дефолт Criterion).
    pub fn new(kind: ServerKind) -> Self {
        Self::with_warm_up(kind, Duration::from_secs(3))
    }

    /// Явное задание длины прогрева — удобно, если группа настроена с другим
    /// `warm_up_time`. Передай `Duration::ZERO`, чтобы записывать с первого сэмпла.
    pub fn with_warm_up(kind: ServerKind, warm_up: Duration) -> Self {
        let server_names: Vec<&'static str> = match kind {
            ServerKind::Postgres => vec!["postgres.exe", "postgres"],
            ServerKind::MarciDB  => vec![
                "marcidb-server.exe", "marcidb_server.exe",
                "marcidb-server",     "marcidb_server",
            ],
        };

        let mut sys = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::new()),
        );
        sys.refresh_processes(ProcessesToUpdate::All);

        let server_pids: Vec<Pid> = sys.processes()
            .values()
            .filter(|p| {
                let n = p.name().to_string_lossy();
                server_names.iter().any(|pat| n.contains(pat))
            })
            .map(|p| p.pid())
            .collect();

        let bench_pid: Option<Pid> = get_current_pid().ok();

        eprintln!(
            "[resource-monitor] server_pids={:?} ({} processes)  bench_pid={:?}  warm_up={:?}",
            server_pids.iter().map(|p| p.as_u32()).collect::<Vec<_>>(),
            server_pids.len(),
            bench_pid.map(|p: Pid| p.as_u32()),
            warm_up,
        );

        if server_pids.is_empty() {
            eprintln!("[resource-monitor] WARN: server process not found — RSS будет 0");
        }

        Self { server_pids, server_names, bench_pid, warm_up }
    }

    fn start(&self) -> (Arc<AtomicBool>, Arc<Mutex<Snapshot>>, JoinHandle<()>) {
        let active   = Arc::new(AtomicBool::new(true));
        let snapshot = Arc::new(Mutex::new(Snapshot::default()));

        let a            = active.clone();
        let s            = snapshot.clone();
        let server_names = self.server_names.clone();
        let bench_pid    = self.bench_pid;
        let warm_up      = self.warm_up;                  // [fix-2]

        // Начальный список PID-ов — будет дополняться в цикле [fix-1].
        let server_pids_init = self.server_pids.clone();

        let handle = thread::spawn(move || {
            let pk = ProcessRefreshKind::new().with_memory();
            let mut sys = System::new_with_specifics(
                RefreshKind::new().with_processes(pk),
            );

            // Первый refresh инициализирует счётчики CPU/памяти.
            sys.refresh_processes_specifics(ProcessesToUpdate::All, pk);

            // [fix-1] server_pids теперь mut — будет пополняться в цикле.
            let mut server_pids: Vec<Pid> = server_pids_init;

            // [fix-2] Засекаем момент старта, чтобы пропускать прогрев.
            let t_start = Instant::now();

            let mut tick: usize = 0;

            while a.load(Ordering::Acquire) {
                sys.refresh_processes_specifics(ProcessesToUpdate::All, pk);

                // ── [fix-1] Периодическое пересканирование PID-ов ────────────
                // Запускаем каждые RESCAN_EVERY тиков и на нулевом тике.
                // ProcessesToUpdate::All уже вызван выше, поэтому таблица свежая.
                if tick % RESCAN_EVERY == 0 {
                    for (pid, p) in sys.processes() {
                        let n = p.name().to_string_lossy();
                        if server_names.iter().any(|pat| n.contains(pat))
                            && !server_pids.contains(pid)
                        {
                            eprintln!(
                                "[resource-monitor] new server PID detected: {} ({})",
                                pid.as_u32(), n
                            );
                            server_pids.push(*pid);
                        }
                    }
                }

                // ── [fix-2] Пропускаем сэмплы во время прогрева ─────────────
                let measuring = t_start.elapsed() >= warm_up;

                if measuring {
                    let mut st = s.lock().unwrap();

                    // Суммируем RSS по всем серверным процессам.
                    let mut srv_rss: u64 = 0;
                    for &pid in &server_pids {
                        if let Some(p) = sys.process(pid) {
                            srv_rss += p.memory();
                        }
                    }
                    if !server_pids.is_empty() {
                        st.server.record(srv_rss);
                    }

                    // Бенчмарк-процесс (включает embedded Prisma engine).
                    if let Some(pid) = bench_pid {
                        if let Some(p) = sys.process(pid) {
                            st.bench.record(p.memory());
                        }
                    }
                }

                tick = tick.wrapping_add(1);
                thread::sleep(Duration::from_millis(SAMPLE_MS));
            }
        });

        (active, snapshot, handle)
    }

    pub fn measure_fn<F>(&self, label: &str, f: F) -> Duration
    where
        F: FnOnce() -> Duration,
    {
        let (active, snapshot, handle) = self.start();
        let elapsed = f();
        active.store(false, Ordering::Release);
        handle.join().expect("resource-monitor thread panicked");
        Self::print_report(label, &snapshot);
        elapsed
    }

    fn print_report(name: &str, snapshot: &Arc<Mutex<Snapshot>>) {
        let mut st = snapshot.lock().unwrap();
        st.server.finalize();
        st.bench.finalize();

        eprintln!(
            // «steady-state» явно отражает, что прогревочные сэмплы пропущены [fix-2].
            "\n[resource] «{}» (steady-state, warm-up excluded)\n\
             \t server : RSS {:6.0} MB avg / {:6.0} MB peak  (n={})\n\
             \t bench  : RSS {:6.0} MB avg / {:6.0} MB peak  (n={})\n\
             \t total  : ~{:.0} MB avg  (server + bench)",
            name,
            st.server.rss_avg, st.server.rss_peak, st.server.n,
            st.bench.rss_avg,  st.bench.rss_peak,  st.bench.n,
            st.server.rss_avg + st.bench.rss_avg,
        );
    }
}

// ─── rbench — drop-in замена group.bench_function ────────────────────────────

pub fn rbench<M, F>(
    group:   &mut BenchmarkGroup<'_, M>,
    name:    impl Into<String>,
    monitor: &ResourceMonitor,
    f:       F,
) where
    M: Measurement,
    F: FnMut(&mut Bencher<'_, M>),
{
    let name = name.into();
    let (active, snapshot, handle) = monitor.start();
    group.bench_function(name.clone(), f);
    active.store(false, Ordering::Release);
    handle.join().expect("resource-monitor thread panicked");
    ResourceMonitor::print_report(&name, &snapshot);
}