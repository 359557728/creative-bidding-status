FROM python:3.7.0-stretch
ENV LANG="en_US.UTF-8"
ADD /app /app
RUN pip3 install --upgrade pip && pip install -r /app/requirements.txt
WORKDIR /app
CMD ["python3", "gdt_creative_drop_reason.py"]


