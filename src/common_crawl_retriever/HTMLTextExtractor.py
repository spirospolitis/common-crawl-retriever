from bs4 import BeautifulSoup

"""
    Implements extraction of text from HTML content.
"""
class HTMLTextExtractor(object):
    def __init__(self):
        pass

    """
        :param html: HTML input.

        :return: text.
    """
    @staticmethod
    def get(html):
        result = ""

        beautiful_soup = BeautifulSoup(html, "html.parser")

        try:
            result += beautiful_soup.head.title.text
        except:
            pass

        try:
            result += beautiful_soup.body.text
        except:
            pass

        return result
