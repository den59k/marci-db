#[derive(Debug,Clone,PartialEq)]
pub struct InsertedIndex {
    pub tree_name: String
}

impl InsertedIndex {
    pub fn tree_name(&self) -> &[u8] {
        return self.tree_name.as_bytes();
    }
}

#[derive(Debug,Clone,PartialEq)]
pub struct InsertedIndexSt {
  // direct индекс - это тот, где ID текущей таблицы стоит первым элементов, а ID внешней таблицы - вторым
  // Вставляем индекс на основе <A.id><B.id>
  pub direct: Option<InsertedIndex>,
  // rev индекс - это тот, где ID внешней таблицы стоит первым полем, а ID текущей таблицы - вторым
  // Вставляем индекс на основе <B.id><A.id>
  pub rev: Option<InsertedIndex>,
  // field индекс - это индекс непосредственно самого поля
  // Вставляем lexical ordered index <field><A.id>
  pub field: Option<InsertedIndex>
}

impl InsertedIndexSt {
    pub fn new() -> InsertedIndexSt {
        return InsertedIndexSt {
            direct: None,
            rev: None,
            field: None
        }
    }
    pub fn is_empty(&self) -> bool {
        return self.direct.is_none() && self.rev.is_none() && self.field.is_none()
    }
}
