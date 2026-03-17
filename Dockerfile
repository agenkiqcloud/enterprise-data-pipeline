FROM public.ecr.aws/lambda/python:3.10

# copy requirements
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy lambda code
COPY datapipeline-lambda.py ${LAMBDA_TASK_ROOT}

# command for lambda
CMD ["datapipeline-lambda.lambda_handler"]