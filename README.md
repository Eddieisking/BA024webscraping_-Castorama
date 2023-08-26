# BA024webscraping_Castorama.fr

This is a web scraping project that focuses on collecting and analyzing data from [Castorama.fr](https://www.castorama.fr/). The goal of the project is to extract customer reviews, date, ratings, and product information from the website for further analysis and insights. The project utilizes the Scrapy framework to automate the process of data collection and processing.

## Project Overview

Web scraping is the process of automatically extracting data from websites. In this project, we aim to gather data for tools brands to gain valuable insights for our analysis. The collected data include ratings, dates, reviews, and other relevant information.

## Objectives

The main objectives of this project are:

- Collect data for mainstream tools brands efficiently and accurately.
- Process and clean the collected data for analysis.
- Store the collected data in an SQL database for management.

## Technologies Used

The following technologies are employed in this project:

- **Python**: The primary programming language used for web scraping and data processing.
- **Scrapy**: A powerful web crawling and scraping framework for Python.
- **MySQL**: A data storage and management library.

## Project Workflow

1. **Data Collection**: We use web scraping techniques to fetch data from target websites. This involves sending HTTP requests, parsing HTML content, and extracting relevant data.
2. **Data Cleaning**: The collected data is often messy and requires cleaning. This step involves removing duplicates, handling missing values, and standardizing formats.
3. **Data translation**: The cleaned data is translated into English for analysis.
4. **Data storage**: The process data is stored in a SQL database for management.

## Getting Started

To run this project on your local machine, follow these steps:

1. Clone this repository: `git clone https://github.com/Eddieisking/BA024webscraping_Castorama.fr.git`
2. Open it in your Python IDE such as Pycharm.
3. Install the required dependencies: `pip install -r requirements.txt` at terminal.
4. Change the code in the spider.py to choose the data you want to collect.
5. Change the config in the items.py and pipelines.py to save the collected data.
6. Run the command at the terminal: `scrapy crawl spider`. 


## Contributions

Contributions to this project are welcome! If you have any suggestions, improvements, or bug fixes, please create an [issue](https://github.com/Eddieisking/BA024webscraping_Castorama.fr/issues) or submit a pull [request](https://github.com/Eddieisking/BA024webscraping_Castorama.fr/pulls).

## Contact

If you have any questions or need further assistance, feel free to contact us at [eddieisking2000@gmail.com].

Happy coding!
