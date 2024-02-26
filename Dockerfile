FROM public.ecr.aws/lambda/python:3.8

COPY . ./

RUN python3 -m pip install -r requirements.txt -t .

CMD ["processor.lambda_handler"]