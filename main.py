from utils import hafas
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

if __name__ == "__main__":

    hf = hafas.Hafas()
    stations = pd.read_csv('./data/stations.csv')

    with ThreadPoolExecutor() as executor:
        future = executor.submit(hf.run_async_loop, stations)
        df = future.result()

    if not df.empty:
        df.to_csv('test.csv', encoding='utf-8-sig')

        delays = hf.get_delays(df)
        delays.to_csv('delays.csv', encoding='utf-8-sig')

    else:
        print("No data was processed successfully.")