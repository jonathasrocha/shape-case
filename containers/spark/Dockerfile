FROM deltaio/delta-docker:0.8.1_2.3.0
  
USER root

COPY containers/spark/requirements.txt ./
RUN pip install -r requirements.txt

RUN touch /var/log/cron.log

# Run cron
ENTRYPOINT ["tail", "-f", "/dev/null"]