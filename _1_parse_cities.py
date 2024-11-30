from selenium import webdriver
from selenium.webdriver.common.by import By
import time

def extract_tables(driver):
    tables = driver.find_elements("css selector", "table.standard.sortable.jquery-tablesorter")
    city_id_to_state_id = []
    city_id_to_name = []
    state_id_to_name = []
    state_set = set()

    for table in tables:
        rows = table.find_elements(By.TAG_NAME, "tr")
        for row in rows[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            city_name = cells[2].text
            state_name = cells[3].text

            if state_name not in state_set:
                state_set.add(state_name)
                state_id_to_name.append(state_name)

            city_id_to_state_id.append(state_id_to_name.index(state_name))
            city_id_to_name.append(city_name)

    return city_id_to_name, city_id_to_state_id, state_id_to_name


def main():
    url = "https://ru.wikipedia.org/wiki/%D0%A1%D0%BF%D0%B8%D1%81%D0%BE%D0%BA_%D0%B3%D0%BE%D1%80%D0%BE%D0%B4%D0%BE%D0%B2_%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D0%B8"
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(4)
    ans = extract_tables(driver)
    driver.quit()
    with open("cities_info.txt", 'w') as f:
        f.write(';'.join(ans[0]) + '\n')
        f.write(';'.join([str(i) for i in ans[1]]) + '\n')
        f.write(';'.join(ans[2]) + '\n')


if __name__ == "__main__":
    main()
