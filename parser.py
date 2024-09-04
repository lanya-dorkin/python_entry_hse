import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm


def parse_book_page(url: str, n_retries: int = 5) -> dict:
    book_data = {
        'name': None,
        'author': [],
        'link': url,
        'rating': None,
        'rating_count': 0,
        'review_count': 0,
        'pages_count': None,
        'price': None,
        'text_reviews': [],
        'age': None,
        'year': None
    }
    
    # не всегда прогружаются авторы
    for _ in range(n_retries):
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        author_tags = soup.find_all('div', itemprop='author')
        for author_tag in author_tags:
            author_name = author_tag.find('span', itemprop='name')
            if author_name:
                book_data['author'].append(author_name.text.strip())
        
        if book_data['author']:
            break

    name_tag = soup.find('h1', itemprop='name')
    if name_tag:
        book_data['name'] = name_tag.text.strip()

    rating_tag = soup.find('div', itemprop='aggregateRating')
    if rating_tag:
        rating_value = rating_tag.find('meta', itemprop='ratingValue')
        if rating_value:
            book_data['rating'] = float(rating_value['content'])
        rating_count = rating_tag.find('meta', itemprop='ratingCount')
        if rating_count:
            book_data['rating_count'] = int(rating_count['content'])

    review_count_tag = soup.find('div', class_='BookFactoids_reviews__qzxey')
    if review_count_tag:
        review_count = review_count_tag.find('span')
        if review_count:
            book_data['review_count'] = int(review_count.text.strip())

    pages_tag = soup.find('div', class_='BookCard_book__preview__data__XjF_j')
    if pages_tag:
        pages_text = pages_tag.find(string=lambda text: 'Объем' in text)
        if pages_text:
            book_data['pages_count'] = int(pages_text.split()[1])

    price_tag = soup.find('meta', itemprop='price')
    if price_tag:
        book_data['price'] = float(price_tag['content'])

    review_tags = soup.find_all('div', class_='Comment_reviewText__PEkHn')
    for review_tag in review_tags:
        review_text = review_tag.find('p')
        if review_text:
            book_data['text_reviews'].append(review_text.text.strip())

    age_tag = soup.find('p', class_='BookCard_book__preview__data__age_rating__s46cA')
    if age_tag:
        book_data['age'] = age_tag.text.strip()

    characteristics_tags = soup.find_all('div', class_='CharacteristicsBlock_characteristic__4pi7v')
    for tag in characteristics_tags:
        title_tag = tag.find('div', class_='CharacteristicsBlock_characteristic__title__atG_Z')
        if title_tag and 'Дата написания' in title_tag.text:
            book_data['year'] = int(tag.text.split()[-1])
            break

    return book_data


def parse_catalog_page(url: str) -> list[str]:
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    return [
        x['href'] for x in soup.find_all('a', attrs={'data-testid': 'art__title'})
    ]
    

def collect_pages(
    base_url: str, genre_href: str, start_page: int = 1,
    n_pages: int = 40, time_sleep: float | int = 1
):
    for cur_page in tqdm(range(start_page, n_pages + 1)):
        page_links = parse_catalog_page(f'{base_url}{genre_href}/?page={cur_page}')
        result = []
        for page_link in page_links:
            book_data = parse_book_page(f'{base_url}{page_link}')
            result.append(book_data)
            time.sleep(time_sleep)
        
        # для сохранения промежуточных "чекпоинтов", если упадет где-то в середине
        yield result


if __name__ == '__main__':
    base_url = 'https://www.litres.ru'
    genre_href = '/genre/programmirovanie-5272'

    try:
        all_books_data = pd.read_csv('parsed_data.csv')
    except FileNotFoundError:
        all_books_data = pd.DataFrame()

    # каждый раз возвращаются все собранные книги, файлик перезаписывается
    for books_data in collect_pages(base_url, genre_href, n_pages=40, time_sleep=0.2):
        all_books_data = pd.concat([all_books_data, pd.DataFrame(books_data)])
        all_books_data.to_csv('parsed_data.csv', index=False)