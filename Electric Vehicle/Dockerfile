FROM python:3.8-slim-buster
ADD electric_vehicle.py /
ADD my_util_pydantic.py /
ADD GeneratedData_DriverBehavior_arr_dep_soc.csv /
ADD ChargingTypeList.csv /
ADD requirements.txt /
RUN pip install -r requirements.txt
CMD [ "python", "./electric_vehicle.py" ]
