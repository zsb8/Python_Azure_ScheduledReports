from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
import numpy as np
import pandas as pd


def init_selenium(url, selenium_server):
    """
    Create a driver of selenium.

    :param str url: Public ZOHO URL, Such as "https://survey.waybase.com/zs/4pRqCP"
    :param str selenium_server: The docker selenium server, such as "http://192.168.1.101:8080"
    :return obj driver:
    """
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Remote(command_executor=selenium_server, options=options)
    driver.get(url)
    return driver


def get_list_language(url, selenium_server):
    """
    The languages that the home page has.

    :param str url: Public ZOHO URL, Such as "https://survey.waybase.com/zs/4pRqCP"
    :param str selenium_server: The docker selenium server, such as "http://192.168.1.101:8080"
    :return list list_language: Such as ['English', 'Français']
    """
    driver = init_selenium(url, selenium_server)
    # print(driver.title)
    home_page_html = driver.execute_script("return document.documentElement.innerHTML;")
    driver.quit()
    home_page_text = home_page_html.replace("\n", "")
    soup = BeautifulSoup(home_page_text, 'html.parser')
    question_pages = soup.findAll('a', {'class': 'langLink'})
    list_language = []
    for i in question_pages:
        list_language.append(i.string)
    return list_language


def answer(ls, question_num, question_id, question_type, language):
    """
    Create a new row for every page.  Only for answer.

    :param list ls: The content of node. Such as [<option device_id="default_drop_devi...]
    :param int question_num: Such as 2.
    :param int question_id: Such as 471831000001010103.
    :param str question_type:  Such as 'single_drop_down', 'multiple_choice', 'likert_rating' or 'single_choice' etc
    :param str language: Such 'English' or 'Français'
    :return list result: Such as [{'questionNum': 26, 'questionId': '471831000001010495', 'langu...]
    """
    ans_count = 1
    list_rows = []
    for item in ls:
        row_answer = {}
        ans_text = item.text.strip()
        row_answer['questionNum'] = question_num
        row_answer['questionId'] = question_id
        row_answer['language'] = language
        row_answer['type'] = 'answer'
        row_answer['subtype'] = question_type
        row_answer['text'] = ans_text
        row_answer['id'] = 'Q' + str(question_num) + '_' + 'A' + str(ans_count)
        ans_count += 1
        list_rows.append(row_answer)
    result = list_rows
    return result


def ans_parent(q, question_num, question_id, question_type, language):
    """
    Scrape the answers.

    :param obj q:
    :param int question_num: Such as 2.
    :param int question_id: Such as 471831000001010103.
    :param str question_type: Such as 'single_drop_down', 'multiple_choice', 'likert_rating' or 'single_choice' etc
    :param str language: Such 'English' or 'Français'
    :return list list_result:
    """
    list_result = []
    answer_parent = q.find('section', {'class': 'sectSavedAns'})
    if 'multiChoiceAns' in answer_parent.attrs['class']:
        answer_ls = answer_parent.findAll('li', {'class', 'choiceItem'})
        list_row = answer(answer_ls, question_num, question_id, question_type, language)
        list_result.extend(list_row)
    if 'dropDownChoice' in answer_parent.attrs['class'] and 'mediaMode' in answer_parent.attrs['class']:
        # use the tag with mediaMode as its less complicated
        answer_ls = answer_parent.findAll('option')
        list_row = answer(answer_ls, question_num, question_id, question_type, language)
        list_result.extend(list_row)
    if 'ratingLikert' in answer_parent.attrs['class']:
        answer_ls = answer_parent.find('div', {'class': 'ratingRowHeader'}).findAll('div', {'class': 'choiceLabel'})
        list_row = answer(answer_ls, question_num, question_id, question_type, language)
        list_result.extend(list_row)
        sub_question_ls = answer_parent.findAll('label', {'class': 'matrixRowMessage'})
        list_row = answer(sub_question_ls, question_num, question_id, question_type, language)
        list_result.extend(list_row)
    if 'matrixChoice' in answer_parent.attrs['class'] and 'mediaMode' in answer_parent.attrs['class']:
        answer_ls = answer_parent.find('ul', {'class': 'choicelist'}).findAll('span', {'class': 'choiceLabel'})
        list_row = answer(answer_ls, question_num, question_id, question_type, language)
        list_result.extend(list_row)
        sub_question_ls = answer_parent.findAll('div', {'class': 'rowLabel'})
        list_row = answer(sub_question_ls, question_num, question_id, question_type, language)
        list_result.extend(list_row)
    return list_result


def judge_homepage(driver):
    """
    Judge whether is homepage with next button.

    :param obj driver:
    :return bool result: True or False
    """
    # msg = driver.find_element(By.NAME, 'continue')
    # print(msg.text)
    click_msg = driver.find_element(By.NAME, 'continue')
    # print(click_msg.text)
    click_msg.click()
    sleep(2)
    msg = driver.find_element(By.NAME, 'continue')
    # print(msg.text)
    if msg.text == "Begin Survey":
        # print("It is the home page.")
        result = True
    else:
        # print("It is the sub content page.")
        result = False
    return result


def scrape_pages_content(driver, language):
    """
    Scrape the content from web page, includes title, title-desc, questions, answers.

    :param obj driver: Selenium driver of one language's pages.
    :param str language: Such as 'English' or 'Français' etc.
    :return list list_result:
    """
    valid_page = True
    page_num = 1
    list_result = []
    q_no = 1
    while valid_page:
        html = driver.execute_script("return document.documentElement.innerHTML;")
        text = html.replace("\n", "").encode('utf-8')
        soup = BeautifulSoup(text, 'html.parser')
        # ------------These are the title part ---------------------
        soup_msg = soup.find(attrs={'name': 'headerMsg'})
        if soup_msg:
            text_title = soup_msg.div['data-revobj-msg']
            row_title = {'questionNum': 0,
                         'questionId': 0,
                         'language': language,
                         'id': 'P' + str(page_num),
                         'type': 'title',
                         'subtype': 'title',
                         'text': text_title
                         }
            list_result.append(row_title)
        soup_msg = soup.find(attrs={'name': 'descMsg'})
        if soup_msg:
            text_desc = soup_msg.string
            row_desc = {'questionNum': 0,
                        'questionId': 0,
                        'language': language,
                        'id': 'P' + str(page_num),
                        'type': 'desc',
                        'subtype': 'desc',
                        'text': text_desc
                        }
            list_result.append(row_desc)
        # ------------These are the question part ---------------------
        questions_list = []
        question_pages = soup.findAll('ul', {'class': 'questionsList'})
        for p in question_pages:
            questions_list += p.findAll('li', {'class': 'surveyQuestion'})
        for q in questions_list:
            row = {}
            # ------------These are the question part ---------------------
            question_num = q_no
            q_no = q_no + 1
            question_id = q.attrs['questionid']  # 471831000001010103
            question_type = q.attrs['questype']  # Such as "single_textbox"
            question_parent = q.find('div', {'class': 'sQuestion'})
            row['questionNum'] = question_num
            row['questionId'] = question_id
            row['language'] = language
            row['id'] = 'Q' + str(row['questionNum'])
            try:
                question_msg_text = question_parent.find('span', {'name': 'questionMsg'}).text.strip()
                # print(question_msg_text)
                row['type'] = 'question'
                row['subtype'] = question_type
            except Exception as error:
                print(error)
                print('skipped:', question_num)
                # question_msg_text = question_parent.text.strip()
                # print(question_msg_text)
                row['type'] = 'text'
                row['subtype'] = np.nan
                continue
            row['text'] = question_msg_text
            list_result.append(row)
            # ------------These are the answer part ---------------------
            """
            Logic for handling  different answer types.
            Such as 'dropDownChoice', 'multiChoiceAns', 'ratingLikert', 'matrixChoice'
            """
            list_ans = ans_parent(q, question_num, question_id, question_type, language)
            list_result.extend(list_ans)
        msg3 = driver.find_element(By.NAME, 'next')
        list_temp = soup.find_all(attrs={'name': 'next'})
        for i in list_temp:
            if str(i).find('style="display: none;"') != -1:
                valid_page = False
        if valid_page is True:
            print(f"This is the {page_num} page--has Next button.")
            msg3.click()
            sleep(2)
        else:
            print(f"This is the {page_num} page--Not found Next button, game ove.")
            valid_page = False
        page_num +=  1
    return list_result


def combine_multi_lang(df):
    """
    The input df has a column named language which includes multi languages.
    Split the input df to some dfs. Every one df only has one language.
    Combine them together with different columns of languages.
    Object is to make different language text are checked clearly.

    :param DataFrame df:
    The columns are : ['Unnamed: 0', 'questionNum', 'questionId', 'language', 'id', 'type', 'subtype', 'text']
    :return DataFrame df_result: The columns such as : ['questionId', 'id', 'type', 'subtype', 'English', 'Français']
    """
    list_lang = df['language'].unique()
    df_result = pd.DataFrame(columns=['questionId', 'id', 'type', 'subtype'])
    for lang in list_lang:
        df_lan = df.loc[df["language"] == lang]
        df_lan = df_lan.rename(columns={'text': lang})
        df_lan.drop(['language', 'questionNum'], axis=1, inplace=True)
        df_result = pd.merge(df_result, df_lan, how='outer', on=['questionId', 'id', 'type', 'subtype'])
    return df_result
