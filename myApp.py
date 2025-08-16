import sys
import json
import random
from mysql.connector import connect
from datetime import datetime, date
import argparse
import time

# Конфигурация подключения к базе данных
DB_CONFIG = {
    'host': 'localhost',
    'user': 'xxxx',     # замените на ваши данные
    'password': 'xxxx',  # замените на ваши данные
    'database': 'employees_db'
}


class Employee:
    def __init__(self, full_name, birth_date_str, gender):
        self.full_name = full_name
        self.birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d').date()
        self.gender = gender

    def save_to_db(self):
        conn = connect(**DB_CONFIG)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO employees (full_name, birth_date, gender) VALUES (%s, %s, %s)",
                (self.full_name, self.birth_date, self.gender)
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def get_age(self):
        today = date.today()
        age = today.year - self.birth_date.year - (
            (today.month, today.day) < (
                self.birth_date.month, self.birth_date.day)
        )
        return age


class EmployeeDirectory:
    def __init__(self):
        self.conn = connect(**DB_CONFIG)

    def create_table(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    full_name VARCHAR(255),
                    birth_date DATE,
                    gender VARCHAR(10)
                )
            """)
            self.conn.commit()
        finally:
            cursor.close()

    def add_employee(self, employee: Employee):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO employees (full_name, birth_date, gender) VALUES (%s, %s, %s)",
                (employee.full_name, employee.birth_date, employee.gender)
            )
            self.conn.commit()
        finally:
            cursor.close()

    def get_all_employees(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT full_name, birth_date, gender FROM employees ORDER BY full_name")
            return cursor.fetchall()
        finally:
            cursor.close()

    def get_employees_by_filters(self, filters=None):
        """
        Получает сотрудников по заданным фильтрам.
        filters — словарь с ключами: gender (str), name_startswith (str)
        """
        query = "SELECT full_name, birth_date, gender FROM employees"
        conditions = []
        params = []

        if filters:
            if 'gender' in filters and filters['gender']:
                conditions.append("gender=%s")
                params.append(filters['gender'])
            if 'name_startswith' in filters and filters['name_startswith']:
                conditions.append("full_name LIKE %s")
                params.append(filters['name_startswith'] + '%')

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY full_name"

        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            cursor.close()

    def close(self):
        self.conn.close()


def generate_employees_from_catalog(file_path='catalog.txt'):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def filter_employees_from_catalog(gender=None, name_startswith=None):
    """
    Загружает данные из catalog.txt и фильтрует по заданным условиям.
    """
    data = generate_employees_from_catalog()

    filtered_data = data

    if gender is not None:
        filtered_data = [
            emp for emp in filtered_data if emp['gender'].lower() == gender.lower()]

    if name_startswith is not None:
        filtered_data = [
            emp for emp in filtered_data
            if emp['full_name'].split()[0].startswith(name_startswith)
        ]

    return filtered_data


def fill_directory_from_catalog(limit=None, gender=None, name_startswith=None):
    data = filter_employees_from_catalog(
        gender=gender, name_startswith=name_startswith)

    if limit is not None:
        data = data[:limit]

    # Распределение по буквам для разнообразия (опционально)
    first_letters = sorted(set(emp['full_name'][0].upper() for emp in data))
    if not first_letters:
        print("Нет данных для вставки по заданным фильтрам.")
        return

    letter_cycle = iter(
        first_letters * ((len(data) // len(first_letters)) + 1))

    directory = EmployeeDirectory()

    for emp in data:
        initial_letter = next(letter_cycle)
        employee_obj = Employee(
            emp['full_name'], emp['birth_date'], emp['gender'])
        directory.add_employee(employee_obj)


def main():
    parser = argparse.ArgumentParser(
        description='Управление сотрудниками и выборками из базы.')

    parser.add_argument('mode', type=str,
                        help='Режим работы: 1 - создать таблицу; 2 - добавить вручную; 3 - список всех; 4 - заполнить по критериям 5 - выполнить выборку по фильтрам')

    # Для режима 2
    parser.add_argument('--full_name', type=str)
    parser.add_argument('--birth_date', type=str)
    parser.add_argument('--gender', type=str)

    # Для режима run_query
    parser.add_argument('--filter_gender', type=str)
    parser.add_argument('--filter_name_start', type=str)

    # Общие параметры
    parser.add_argument('--limit', type=int)

    args = parser.parse_args()

    start_time = time.time()

    mode = args.mode

    if mode == '1':
       directory = EmployeeDirectory()
       directory.create_table()
       print("Таблица создана или уже существует.")

    elif mode == '2':
       # Проверка обязательных параметров
       if not all([args.full_name, args.birth_date, args.gender]):
           print("Для добавления необходимо указать --full_name --birth_date --gender")
           return
       try:
           datetime.strptime(args.birth_date, '%Y-%m-%d')
       except ValueError:
           print("Некорректный формат даты. Используйте ГГГГ-ММ-ДД.")
           return

       employee = Employee(args.full_name, args.birth_date, args.gender)
       directory = EmployeeDirectory()
       employee.save_to_db()
       directory.close()
       print(
           f"Добавлена запись: {args.full_name}, {args.birth_date}, {args.gender}")

    elif mode == '3':
       directory = EmployeeDirectory()
       employees = directory.get_all_employees()
       today = date.today()
       for full_name, birth_date_obj, gender in employees:
           birth_date_str = birth_date_obj.strftime('%Y-%m-%d')
           age = today.year-birth_date_obj.year - \
               ((today.month, today.day) < (birth_date_obj.month, birth_date_obj.day))
           print(f"{full_name} | {birth_date_str} | {gender} | {age} лет")
       directory.close()
    
    elif mode == '4':
       # Заполнение из catalog.txt с фильтрами и лимитом
       fill_directory_from_catalog(limit=args.limit,
                                   gender=args.filter_gender,
                                   name_startswith=args.filter_name_start)
       print('Данные из catalog.txt успешно добавлены с учетом фильтров.')
    elif mode == '5':
       # Формируем фильтры из аргументов командной строки
       filters = {}
       if args.filter_gender:
           filters['gender'] = args.filter_gender
       if args.filter_name_start:
           filters['name_startswith'] = args.filter_name_start

       directory = EmployeeDirectory()

       # Получаем выборку по фильтрам из базы
       results = directory.get_employees_by_filters(filters)

       today = date.today()

       for full_name, birth_date_obj, gender in results:
           birth_date_str = birth_date_obj.strftime('%Y-%m-%d')
           age = today.year-birth_date_obj.year - \
               ((today.month, today.day) < (birth_date_obj.month, birth_date_obj.day))
           print(f"{full_name} | {birth_date_str} | {gender} | {age} лет")

       directory.close()


    else:
       print('Некорректный режим. Используйте 1/2/3/4/5.')

    end_time=time.time()
    print(f"Время выполнения: {end_time - start_time:.2f} секунд.")

if __name__=='__main__':
   main()