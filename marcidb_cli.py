#!/usr/bin/env python3
"""
CLI клиент для работы с MarciDB через HTTP API.
Использование:
    python marcidb_cli.py <команда> <модель> [<json-данные>]
Примеры:
    python marcidb_cli.py insert User '{"name": "John", "surname": "Doe", "email": "john@example.com"}'
    python marcidb_cli.py findMany User
    python marcidb_cli.py findMany User '{"$where": {"email": "john@example.com"}}'
    python marcidb_cli.py update User '{"id": 1, "name": "Johnny"}'
    python marcidb_cli.py delete User '{"id": 1}'
    python marcidb_cli.py index User
"""

import json
import sys
import requests
from typing import Optional, Dict, Any
import re


class MarciDBClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 3000):
        self.base_url = f"http://{host}:{port}"

    def send_request(self, method: str, model: str, action: str, data: Optional[Dict[str, Any]] = None) -> Dict[
        str, Any]:
        """Отправляет запрос к API"""
        url = f"{self.base_url}/{model}/{action}"

        if method.upper() == "GET":
            response = requests.get(url)
        elif method.upper() == "POST":
            response = requests.post(url, json=data)
        else:
            raise ValueError(f"Неподдерживаемый метод: {method}")

        response.raise_for_status()

        # Пытаемся распарсить JSON, если это возможно
        try:
            return response.json()
        except:
            return {"raw_response": response.text}

    def insert(self, model: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Вставка документа"""
        return self.send_request("POST", model, "insert", data)

    def find_many(self, model: str, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Поиск документов"""
        if query:
            return self.send_request("POST", model, "findMany", query)
        else:
            return self.send_request("GET", model, "findMany")

    def update(self, model: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Обновление документа"""
        return self.send_request("POST", model, "update", data)

    def delete(self, model: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Удаление документа"""
        return self.send_request("POST", model, "delete", data)

    def index(self, model: str) -> Dict[str, Any]:
        """Создание векторного индекса"""
        return self.send_request("POST", model, "index", {})


def parse_json_like_string(json_str):
    """
    Пытается распарсить строку, которая может содержать JSON с одинарными кавычками.
    Сначала пробует стандартный JSON парсинг, затем преобразует одинарные кавычки в двойные.
    """
    # Пробуем стандартный JSON парсинг
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        pass

    # Пробуем заменить одинарные кавычки на двойные (с некоторыми предосторожностями)
    # Это простая эвристика, не покрывающая все случаи

    # Заменяем одинарные кавычки на двойные, но не внутри уже существующих двойных кавычек
    # и не экранированные одинарные кавычки
    converted = json_str

    # Простая замена, если нет смешанных кавычек
    if "'" in converted and '"' not in converted:
        converted = converted.replace("'", '"')
        try:
            return json.loads(converted)
        except json.JSONDecodeError:
            pass

    # Пробуем ast.literal_eval как запасной вариант
    try:
        import ast
        return ast.literal_eval(json_str)
    except (SyntaxError, ValueError):
        pass

    # Если ничего не помогло
    raise json.JSONDecodeError(f"Не удалось распарсить строку: {json_str}", json_str, 0)


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()
    model = sys.argv[2]
    json_data = None

    # Парсим JSON данные, если они предоставлены
    if len(sys.argv) > 3:
        # Объединяем все оставшиеся аргументы в одну строку
        json_str = ' '.join(sys.argv[3:])

        # Убираем внешние кавычки, если они есть (для Windows)
        json_str = json_str.strip()
        if (json_str.startswith("'") and json_str.endswith("'")) or \
                (json_str.startswith('"') and json_str.endswith('"')):
            json_str = json_str[1:-1]

        try:
            json_data = parse_json_like_string(json_str)
        except json.JSONDecodeError as e:
            print(f"Ошибка парсинга JSON: {e}")
            print(f"Полученная строка: {json_str}")
            print("\nСоветы по формату:")
            print('1. Используйте двойные кавычки для JSON: "{\\"name\\": \\"John\\"}"')
            print('2. Или используйте одинарные кавычки снаружи: \'{"name": "John"}\'')
            print('3. В PowerShell: "{\\"name\\": \\"John\\"}" или \'{"name": "John"}\'')
            sys.exit(1)

    client = MarciDBClient()

    try:
        if command == "insert":
            if not json_data:
                print("Для команды insert требуется JSON данные")
                sys.exit(1)
            result = client.insert(model, json_data)

        elif command == "findmany":
            result = client.find_many(model, json_data)

        elif command == "update":
            if not json_data:
                print("Для команды update требуется JSON данные")
                sys.exit(1)
            result = client.update(model, json_data)

        elif command == "delete":
            if not json_data:
                print("Для команды delete требуется JSON данные")
                sys.exit(1)
            result = client.delete(model, json_data)

        elif command == "index":
            result = client.index(model)

        else:
            print(f"Неизвестная команда: {command}")
            print("Доступные команды: insert, findMany, update, delete, index")
            sys.exit(1)

        # Красиво выводим результат
        print(json.dumps(result, indent=2, ensure_ascii=False))

    except requests.exceptions.RequestException as e:
        print(f"Ошибка HTTP запроса: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()