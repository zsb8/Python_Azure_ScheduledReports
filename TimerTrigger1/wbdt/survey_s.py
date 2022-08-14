import traceback
import pandas as pd
from time import sleep
from selenium.webdriver.common.by import By
from .utils import functions_s_survery as functions


def scrape(url, selenium_server):
    """
    Main program.

    :param str url: Public ZOHO URL, Such as "https://survey.waybase.com/zs/4pRqCP"
    :param str selenium_server: The docker selenium server, such as "http://192.168.1.101:8080"
    :return DataFrame df or None:
    """
    result = None
    list_results = []
    list_language = functions.get_list_language(url, selenium_server)
    print(f"This home page has {len(list_language)} languages: {list_language}.")
    for language in list_language:
        print("-"*30)
        driver = functions.init_selenium(url, selenium_server)
        try:
            home_page_msg_element = driver.find_element(By.LINK_TEXT, language)
            home_page_msg_element.click()
            sleep(2)
            if not functions.judge_homepage(driver):
                print(f"Begin to scrape the '{language}' survey contents, they lie in some pages.")
                list_result = functions.scrape_pages_content(driver, language)
                list_results.extend(list_result)
        except Exception as error:
            print({error})
            print('Error happened. '.center(30, '-'))
            print(traceback.format_exc())
        driver.quit()
    if len(list_results) > 0:
        df = pd.DataFrame(list_results)
        df2 = functions.combine_multi_lang(df).drop_duplicates()
        df2.reset_index(drop=True, inplace=True)
        result = df2
    return result


if __name__ == "__main__":
    my_url = "https://survey.waybase.com/zs/4pRqCP"
    my_selenium_server = "http://192.168.1.101:8080"
    my_df = scrape(my_url, my_selenium_server)
    print(my_df)
    my_df.to_csv('d:/survey.csv')

