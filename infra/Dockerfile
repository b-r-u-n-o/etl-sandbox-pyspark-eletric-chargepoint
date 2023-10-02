FROM jupyter/pyspark-notebook

WORKDIR /home/bruno

VOLUME [ "/home/bruno"]

RUN pip install pyspark

COPY /input/electric-chargepoints-2017.csv /home/bruno/input/

EXPOSE 8888

CMD [ "jupyter", "lab" ]