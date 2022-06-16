FROM python:3.10.4-slim-bullseye
ARG NPM_GITHUB_READ
ENV NPM_GITHUB_READ=$NPM_GITHUB_READ
RUN apt-get update && apt-get -y install gcc g++
WORKDIR /usr/src/app
COPY . .
RUN pip install -r requirements.txt
# connect the repository to the container
LABEL org.opencontainers.image.source https://github.com/zabkwak/mat-facebook-ads-transformer
CMD [ "python", "./" ]
