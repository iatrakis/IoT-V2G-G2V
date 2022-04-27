FROM python:3.8-slim-buster
ADD charging_station.py /
ADD my_util_pydantic.py /
ADD Generated_Data_EnergySourcesLoad-KDE-4year.csv /
ADD GeneratedData_DriverBehavior_1.csv /
ADD ChargingTypeList.csv /
ADD requirements.txt /
RUN pip install -r requirements.txt
CMD [ "python", "./charging_station.py" ]
