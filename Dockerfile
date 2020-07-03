FROM python:3
ENV LANG="en_US.UTF-8"
RUN python -m venv /opt/venv-python
RUN /opt/venv-python/bin/pip install --index-url https://mirrors.aliyun.com/pypi/simple/ pex
RUN mkdir /opt/wheels /opt/pex
ADD /app /app
RUN /opt/venv-python/bin/pip install --index-url https://mirrors.aliyun.com/pypi/simple/ --upgrade pip wheel && /opt/venv-python/bin/pip wheel --index-url https://mirrors.aliyun.com/pypi/simple/ --wheel-dir /opt/wheels -r /app/requirements.txt
RUN /opt/venv-python/bin/pex --no-index --find-links /opt/wheels -r /app/requirements.txt -o /opt/pex/base.pex
FROM python:3-slim
COPY --from=0 /opt/pex /opt/pex
ADD /app /app
WORKDIR /app
CMD ["/opt/pex/base.pex", "gdt_creative_drop_reason.py"]
