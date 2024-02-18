import csv
import os
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class StockDataProcessor:
    def __init__(self, config):
        self.config = config

    def read_csv(self, file_path):
        """
            Читает данные из CSV-файла и возвращает их в виде списка словарей.
            file_path: string Путь к файлу CSV, который необходимо прочитать.
        """
        try:
            with open(file_path, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                return list(csv_reader)
        except Exception as e:
            logging.error(f"Ошибка при чтении файла {file_path}: {e}")
            return None

    def find_extreme_values(self, csv_data):
        """
        Находит экстремальные значения (минимальное и максимальное) в данных акций и проверяет, удовлетворяют ли они заданному порогу изменений.
        csv_data: Список словарей с данными акций.
        """
        if not csv_data:
            return None

        min_low = min(float(row['low']) for row in csv_data)
        max_high = max(float(row['high']) for row in csv_data)
        percentage_change = (max_high - min_low) / min_low * 100

        if self.config['MIN_PERCENTAGE_CHANGE_THRESHOLD'] <= percentage_change <= self.config['MAX_PERCENTAGE_CHANGE_CONDOR']:
            return min_low, max_high
        return None


    def process_tickers(self, tickers):
        """
        TODO:
        TODO:
        """
        results = []

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.process_single_ticker, ticker) for ticker in tickers]
            for future in futures:
                result = future.result()
                if result:
                    results.append(result)

        self.save_results(results)

    def process_single_ticker(self, ticker):
        '''
        Обрабатывает данные для одного тикера, находя его экстремальные значения и определяя, подходят ли они под заданные критерии.
        ticker: string (Тикер акции для обработки.)
        '''
        file_path = os.path.join(self.config['TICKERS_CANDLES_DATA'], f'{ticker}_stock_1d_data.csv')
        if not os.path.exists(file_path):
            logging.warning(f"Файл не найден: {file_path}")
            return None

        csv_data = self.read_csv(file_path)
        extreme_values = self.find_extreme_values(csv_data)
        if extreme_values:
            min_low, max_high = extreme_values
            return {'Ticker': ticker, 'Min Low': min_low, 'Max High': max_high}

        return None

    def save_results(self, results):
        OUTPUT_FILE_path = os.path.join(self.config['OUTPUT_FOLDER'], self.config['OUTPUT_FILE'])
        with open(OUTPUT_FILE_path, 'w', newline='') as csv_output:
            fieldnames = ['Ticker', 'Min Low', 'Max High']
            writer = csv.DictWriter(csv_output, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                writer.writerow(result)

class ProcessTickersExtremum(StockDataProcessor):
    def TODO():
        pass
    def TODO():
        """
        Функция должна анализировать предоставленный временной ряд, идентифицировать и выбирать последовательности экстремумов: максимум, минимум, максимум, минимум, 
        (максимумы и минимумы могут отличаться от других на 1.5 процента)

        """
        pass  


def outer():
    # напишите декоратор для оценки времени выполнении программы
    pass


if __name__ == "__main__":
    config = {
        'EXTREMUM_COUNT': 4,
        'TICKERS_CANDLES_DATA': r'.\tickers_data',
        'OUTPUT_FOLDER': r'.\output',
        'OUTPUT_FILE': r'result.csv',
        'MIN_PERCENTAGE_CHANGE_THRESHOLD': 5,
        'TICKERS_FILE': r'.\valid_tickers.txt',
        'MAX_PERCENTAGE_CHANGE_CONDOR': 20
    }
    processor = StockDataProcessor(config)
    with open(config['TICKERS_FILE'], "r") as file:
        tickers = [line.strip() for line in file.readlines()]
    processor.process_tickers(tickers)
