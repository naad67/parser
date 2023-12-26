FROM joyzoursky/python-chromedriver:3.8
RUN pip install --upgrade pip
WORKDIR /src
COPY . .
RUN pip install -r requirements.txt


CMD ["python", "krisha.py"]