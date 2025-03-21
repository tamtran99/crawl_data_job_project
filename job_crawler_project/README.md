# Job Crawler Project

**Description**:  
This project aims to create a Crawler to auto-get job information from hiring platforms like Linkedin, topcv, etc. After that, the data will be analyzed, and a notification will be sent when the job matches the setting.

![Project Screenshot](img/example.png) 

## TODO

- [x] **Feature 1**: Create a crawler to get information jobs from LinkedIn and store them in the database.
- [ ] **Feature 2**: Create a crawler to get information jobs from another hiring platform.
- [ ] **Feature 3**: Send analytics report
- [ ] **Feature 4**: Send notifications when found matching jobs.

## Installation

Follow these steps to install and set up the project locally.

- Install all requirements in requirements.txt
- Clone <code>copy.ini.example</code> to <code>copy.ini</code> and replace with your config.
- Run <code>init.py</code> **if the first run** to create table in DB.
- Run <code>crawl_data.py</code> to crawl job.