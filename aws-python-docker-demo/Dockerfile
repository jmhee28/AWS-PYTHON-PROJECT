FROM public.ecr.aws/lambda/python:3.7
RUN yum update -y
RUN yum install -y \
    python3-pip \
    zip \
    RUN yum -y clean all
RUN python3.7 -m pip install --upgrade pip
RUN pip install pandas
COPY app.py ./

# You can overwrite command in `serverless.yml` template
CMD ["app.handler"]
