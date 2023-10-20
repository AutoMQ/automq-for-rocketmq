FROM amazoncorretto:21
WORKDIR /root
RUN yum install which bash -y
ADD rocketmq-on-s3.tar.gz .
CMD ["/root/rocketmq-on-s3/bin/start-broker.sh"]