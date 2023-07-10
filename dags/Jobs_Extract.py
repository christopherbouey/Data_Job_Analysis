from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
import psycopg2
from datetime import datetime


def LI_load_jobs(driver):
  # Set job count from job search page - to be used for loading new listings enough times
  job_count = int(driver.find_element('css selector','h1>span').get_attribute('innerText'))

  # Load all job listings into html
  i = 0
  while i <= int(job_count/25):
      driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
      i = i + 1
      try:
          driver.find_element('xpath','//button[contains(text(),"See more jobs")]').click()
          time.sleep(3)
      except:
          pass
          time.sleep(3)

  job_list = driver.find_element('class name','jobs-search__results-list')
  jobs = job_list.find_elements('tag name','li')
  # TEST USAGE
  # jobs = jobs[:50]
  return jobs

def LI_scrape_jobs(driver, jobs):
  # Pull initial job info
  job_ids = []
  job_titles = []
  companies = []
  locations = []
  post_dates = []
  job_links = []

  for job in jobs:
    job_id = job.find_element('tag name','div').get_attribute('data-entity-urn').split(':')[-1]
    job_ids.append(job_id)

    job_title = job.find_element('xpath',".//h3[@class='base-search-card__title']").get_attribute('innerText')
    job_titles.append(job_title)

    company = job.find_element('xpath',".//h4[@class='base-search-card__subtitle']").get_attribute('innerText')
    companies.append(company)

    location = job.find_element('xpath',".//span[@class='job-search-card__location']").get_attribute('innerText')
    locations.append(location)

    post_date = job.find_element('xpath',".//time").get_attribute('datetime')
    post_dates.append(post_date)

    job_link = job.find_element('css selector','a').get_attribute('href')
    job_links.append(job_link)


  #For test purposes, save html file
  # with open('test.txt', 'w') as f:
  #   html = driver.page_source
  #   f.write(html)

  # Cycle through each job listing and pull more info
  job_descs = []
  seniorities = []
  emp_types = []
  industries = []

  for item in range(1,len(jobs)+1):
    job_func = []
    industry = []
    # Click on job posting to view details
    driver.find_element('xpath',f'/html/body/div/div/main/section[2]/ul/li[{item}]/div').click()
    time.sleep(3)
    # Pull job description from listing
    job_desc_path = '/html/body/div/div/section/div[2]/div/section/div/div/section/div'
    try:
      job_desc = driver.find_element('xpath',job_desc_path).get_attribute('innerText')
    except Exception as err:
      job_desc = None
      print(f'{item}: Job Desc')
    job_descs.append(job_desc)
    # Pull seniority type from listing
    sen_path = '//h3[contains(text(),"Seniority level")]/following-sibling::span'
    try:
      seniority = driver.find_element('xpath',sen_path).get_attribute('innerText')
    except Exception as err:
      seniority = None
      print(f'{item}: Seniority')
    seniorities.append(seniority)
    # Pull employment type
    emp_type_path = '//h3[contains(text(),"Employment type")]/following-sibling::span'
    try:
      emp_type  = driver.find_element('xpath',emp_type_path).get_attribute('innerText')
    except Exception as err:
      emp_type = None
      print(f'{item}: Emp Type')
    emp_types.append(emp_type)
    # Pull industries
    industry_path = '//h3[contains(text(),"Industries")]/following-sibling::span'
    try:
      industry = driver.find_element('xpath',industry_path).get_attribute('innerText')
    except Exception as err:
      industry = None
      print(f'{item}: Industry')
    industries.append(industry)

  # Load into DF
  job_data = pd.DataFrame({'Create_Date': pd.to_datetime("today").date(),
    'ID': job_ids,
    'Post_Date': post_dates,
    'Company': companies,
    'Title': job_titles,
    'Location': locations,
    'Description': job_descs,
    'Level': seniorities,
    'Type': emp_types,
    'Industry': industries,
    'Link': job_links
    })

  # Clean description text
  job_data['Description'] = job_data['Description'].str.replace('\n',' ')

  return job_data

def job_extraction():
  # Set URL for linked job search
  URL = "https://www.linkedin.com/jobs/search?keywords=Data%20Engineer&location=United%20States&locationId=&geoId=103644278&f_TPR=&f_PP=102277331&f_JT=F&f_SB2=4&position=1&pageNum=0"

  # Set driver options
  options = webdriver.ChromeOptions()
  options.add_argument('--ignore-ssl-errors=yes')
  options.add_argument('--ignore-certificate-errors')
  options.add_argument('--disable-dev-shm-usage')
  options.add_argument('--no-sandbox')
  options.add_argument('-headless')
  options.add_argument('window-size=2000x1500')
  # Set driver loc and initiate Chrome Driver
  remote_webdriver = 'remote_chromedriver'
  with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as wd:

  # Maximize window - smaller windows behave differently on clicks and will break script
    wd.maximize_window()
    wd.get(URL)

    jobs = LI_load_jobs(wd)
    jobs_df = LI_scrape_jobs(wd, jobs)
    jobs_df.to_csv(f'/opt/airflow/dags/data/LIJobs_working.csv', index=False)

if __name__=='__main__':
  job_extraction()