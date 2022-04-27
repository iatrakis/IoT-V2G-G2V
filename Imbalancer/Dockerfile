FROM python:3.8-slim-buster
ADD imbalancer.py /
ADD my_util_pydantic.py /
ADD Generated_Data_EnergySourcesLoad-KDE-4year.csv /
ADD ChargingTypeList.csv /
ADD requirements.txt /
RUN pip install -r requirements.txt
CMD [ "python", "./imbalancer.py" ]
