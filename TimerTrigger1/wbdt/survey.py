"""
The mapper module contains a webscraper for Zoho surveys that can be used to create a mapping file between the base English file
to other translations. In this version it depends on downloading the page to an HTML.
For future update: There are idiosyncrasies with the way the data is being structured and formatted.
"""
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


def extract_text(file_path, language):
    """
    Extract the data from the HTML file
    :param str file_path: The URL path to the HTML file.
    :param str language: language of the html file [en / fr]
    :return result: a list of questions, sub questions and answers extracted
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        text = file.read().replace('\n', '')
    soup = BeautifulSoup(text, 'html.parser')
    result = []
    # Iterate through each question section
    questions_list = []
    question_pages = soup.findAll('ul', {'class': 'questionsList'})
    for p in question_pages:
        questions_list += p.findAll('li', {'class': 'surveyQuestion'})
    q_no = 1
    for q in questions_list:
        row = {}
        question_num = q_no
        q_no = q_no + 1
        # ------------These are the question part ---------------------
        question_id = q.attrs['questionid']  # 471831000001010103
        question_type = q.attrs['questype']  # Such as "single_textbox"
        question_parent = q.find('div', {'class': 'sQuestion'})
        row['questionNum'] = question_num
        row['questionId'] = question_id
        row['id'] = 'Q' + str(row['questionNum'])
        try:
            question_msg_text = question_parent.find('span', {'name': 'questionMsg'}).text.strip()
            # print(question_msg_text)
            row['type'] = 'question'
            row['subtype'] = question_type
        except Exception as error:
            print(error)
            print('skipped:', question_num)
            question_msg_text = question_parent.text.strip()
            # print(question_msg_text)
            row['type'] = 'text'
            row['subtype'] = np.nan
            continue
        row[language] = question_msg_text
        result.append(row)
        # ------------These are the answer part ---------------------
        """
        Logic for handling  different answer types.
        Such as 'dropDownChoice', 'multiChoiceAns', 'ratingLikert', 'matrixChoice'
        """
        def _get_new_row(ls):
            ans_count = 1
            for item in ls:
                row_answer = {}
                ans_text = item.text.strip()
                row_answer['questionNum'] = question_num
                row_answer['questionId'] = question_id
                row_answer['type'] = 'answer'
                row_answer['subtype'] = question_type
                row_answer[language] = ans_text
                row_answer['id'] = 'Q' + str(row['questionNum']) + '_' + 'A' + str(ans_count)
                ans_count += 1
                result.append(row_answer)
        answer_parent = q.find('section', {'class': 'sectSavedAns'})
        if 'multiChoiceAns' in answer_parent.attrs['class']:
            answer_ls = answer_parent.findAll('li', {'class', 'choiceItem'})
            _get_new_row(answer_ls)
        if 'dropDownChoice' in answer_parent.attrs['class'] and 'mediaMode' in answer_parent.attrs['class']:
            # use the tag with mediaMode as its less complicated
            answer_ls = answer_parent.findAll('option')
            _get_new_row(answer_ls)
        if 'ratingLikert' in answer_parent.attrs['class']:
            answer_ls = answer_parent.find('div', {'class': 'ratingRowHeader'}).findAll('div', {'class': 'choiceLabel'})
            _get_new_row(answer_ls)
            sub_question_ls = answer_parent.findAll('label', {'class': 'matrixRowMessage'})
            _get_new_row(sub_question_ls)
        if 'matrixChoice' in answer_parent.attrs['class'] and 'mediaMode' in answer_parent.attrs['class']:
            answer_ls = answer_parent.find('ul', {'class': 'choicelist'}).findAll('span', {'class': 'choiceLabel'})
            _get_new_row(answer_ls)
            sub_question_ls = answer_parent.findAll('div', {'class': 'rowLabel'})
            _get_new_row(sub_question_ls)
    return result


def main(path_en, path_fr):
    """
    This is the main program. Input the en and fr path, scrape the questions and answers,
    output a data set which compare the english and french language text.

    :param str path_en: Such as "/temp/survey-eng.html"
    :param str path_fr: Such as "/temp/survey-fr.html"
    :return Dataframe df_out: Columns are ['id', 'questionId', 'questionNum', 'type', 'subtype', 'en', 'fr']
    """
    # Parse English webpage
    df_en = pd.DataFrame(extract_text(path_en, 'en'))
    # df_en.to_csv("English_Survey.csv")
    # Parse French webpage
    df_fr = pd.DataFrame(extract_text(path_fr, 'fr'))
    # df_fr.to_csv("French_Survey.csv")
    # Merge the two outputs
    df_out = df_en.merge(df_fr, how='inner', on='id', suffixes=('', '_y'))
    # Remove duplicating columns after the merge
    df_out.drop(df_out.filter(regex='_y$').columns.tolist(), axis=1, inplace=True)
    # reorder
    df_out = df_out[['id', 'questionId', 'questionNum', 'type', 'subtype', 'en', 'fr']]
    return df_out


if __name__ == "__main__":
    my_path_en = 'survey/survey-eng.html'
    my_path_fr = 'survey/survey-fr.html'
    main(my_path_en, my_path_fr)
