#!/usr/bin/env python3
import os
import time
import requests
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Optional

# Конфигурация
BASE_URL = "https://api.hh.ru/vacancies"
AREA_ID = 113  # ID для Сыктывкара
EXCEL_PATH = r"C:\Users\pavel\PycharmProjects\txx\vacancies_syktu.xlsx"
MAX_RETRIES = 3
REQUEST_DELAY = 2  # Задержка между запросами в секундах
MAX_WAIT_TIME = 30  # Максимальное время ожидания освобождения файла

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def ensure_folder_exists(filepath: str) -> None:
    """Создаёт каталог для файла, если его не существует."""
    folder = os.path.dirname(filepath)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)
        logger.info(f"Создан каталог: {folder}")

def is_file_locked(filepath: str) -> bool:
    """Проверяет, можно ли открыть файл для записи."""
    if not os.path.exists(filepath):
        return False
    try:
        with open(filepath, 'a'):
            return False
    except IOError:
        return True

def wait_for_file(filepath: str, wait_time: int = 2, total_wait: int = MAX_WAIT_TIME) -> bool:
    """Ожидает освобождения файла."""
    start_time = time.time()
    while time.time() - start_time < total_wait:
        if not is_file_locked(filepath):
            return True
        logger.warning("Excel файл занят, ожидание освобождения...")
        time.sleep(wait_time)
    return False

def format_salary(salary: Optional[Dict]) -> str:
    """Форматирует данные о зарплате."""
    if not salary:
        return "не указана"
    
    salary_from = salary.get("from")
    salary_to = salary.get("to")
    currency = salary.get("currency", "")
    
    if salary_from and salary_to:
        return f"{salary_from} - {salary_to} {currency}"
    elif salary_from:
        return f"от {salary_from} {currency}"
    elif salary_to:
        return f"до {salary_to} {currency}"
    return "не указана"

def fetch_vacancies() -> List[Dict]:
    """Собирает вакансии с hh.ru для Сыктывкара."""
    vacancies = []
    page = 0
    total_pages = 1
    logger.info("Начинаем сбор вакансий с hh.ru...")

    while page < total_pages:
        params = {
            "area": AREA_ID,
            "page": page,
            "per_page": 100,
            "text": "сыктывкар"  # Дополнительная фильтрация по тексту
        }
        
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(BASE_URL, params=params, timeout=10)
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', REQUEST_DELAY))
                    logger.warning(f"Превышен лимит запросов. Ожидание {retry_after} секунд...")
                    time.sleep(retry_after)
                    continue
                    
                response.raise_for_status()
                data = response.json()
                success = True
                break
                
            except requests.RequestException as e:
                logger.error(f"Ошибка запроса (страница {page}, попытка {attempt}): {str(e)}")
                if attempt < MAX_RETRIES:
                    time.sleep(REQUEST_DELAY * attempt)
                continue

        if not success:
            logger.error(f"Пропуск страницы {page} после неудачных попыток")
            page += 1
            continue

        total_pages = data.get("pages", 0)
        items = data.get("items", [])
        
        for item in items:
            try:
                area_info = item.get("area", {})
                city = area_info.get("name", "").lower()
                
                # Проверяем, что вакансия действительно из Сыктывкара
                if "сыктывкар" not in city:
                    continue

                vacancy_data = {
                    "Название вакансии": item.get("name", "Нет названия"),
                    "Работодатель": item.get("employer", {}).get("name", "Не указан"),
                    "Зарплата": format_salary(item.get("salary")),
                    "Город": city,
                    "Ссылка": item.get("alternate_url", "Нет ссылки")
                }
                
                vacancies.append(vacancy_data)
                logger.info(f"✔️ Добавлена вакансия: {vacancy_data['Название вакансии']}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка обработки вакансии: {str(e)}")
                continue

        page += 1
        time.sleep(REQUEST_DELAY)

    logger.info(f"Сбор вакансий завершён. Всего собрано: {len(vacancies)}")
    return vacancies

def save_to_excel(vacancies: List[Dict], filepath: str) -> None:
    """Сохраняет вакансии в Excel-файл."""
    ensure_folder_exists(filepath)
    
    if os.path.exists(filepath) and not wait_for_file(filepath):
        logger.error("Excel файл недоступен для записи. Работа завершается.")
        return

    try:
        df = pd.DataFrame(vacancies)
        df.to_excel(filepath, index=False, engine='openpyxl')
        logger.info(f"✔️ Данные успешно сохранены в {filepath}")
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении Excel: {str(e)}")

def main():
    try:
        start_time = datetime.now()
        logger.info("Запуск скрипта...")
        
        vacancies = fetch_vacancies()
        if vacancies:
            save_to_excel(vacancies, EXCEL_PATH)
        
        execution_time = datetime.now() - start_time
        logger.info(f"Скрипт завершил работу за {execution_time}")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")

if __name__ == "__main__":
    main()
