FROM apache/hadoop:3

USER root

RUN yum install -y java-1.8.0-openjdk-devel && \
    yum clean all

USER hadoop

CMD ["/bin/bash"]
