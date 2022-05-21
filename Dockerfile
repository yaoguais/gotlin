FROM alpine

LABEL maintainer yaoguais <newtopstdio@163.com>

COPY ./gotlin ./program.json ./schema.sql /usr/local/gotlin/

WORKDIR /usr/local/gotlin

ENV PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/gotlin"

CMD ["gotlin", "start"]
