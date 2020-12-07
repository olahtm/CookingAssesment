FROM ubuntu:18.04

ADD . /cooking

RUN apt update

RUN apt install -y openjdk-8-jdk openjfx maven zip curl

RUN curl -s "https://get.sdkman.io" | bash
RUN /bin/bash -c "source $HOME/.sdkman/bin/sdkman-init.sh; sdk install java 8.0.272.fx-zulu; cd /cooking ; mvn clean install -DskipTests"
