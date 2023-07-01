FROM public.ecr.aws/lambda/python:3.7
RUN yum update -y
RUN yum install -y \
    python3-pip \
    zip \
    RUN yum -y clean all
RUN python3.7 -m pip install --upgrade pip
RUN pip install asyncio
RUN pip install python-dotenv
RUN pip install scikit-learn
RUN pip install pandas
RUN pip install scipy
RUN pip install matplotlib
RUN pip install boto3


COPY ./* ./

# You can overwrite command in `serverless.yml` template
CMD ["handler.analysis"]
