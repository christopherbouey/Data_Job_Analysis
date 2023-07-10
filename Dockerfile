FROM apache/airflow:2.6.1
USER airflow
RUN pip3 install selenium && \
    pip3 install bs4 && \
    pip3 install lxml && \
    pip3 install selenium-stealth