FROM amazoncorretto:21
WORKDIR /root
RUN yum install which bash -y
ADD automq-for-rocketmq.tar.gz .
CMD ["/root/automq-for-rocketmq/bin/start-broker.sh"]