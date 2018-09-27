FROM debian:stretch

# set up environment
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    texlive \
    texlive-lang-german \
    texlive-latex-extra \
    texlive-xetex \
    texlive-luatex \
    texlive-latex-base \
    lmodern \
    fonts-roboto \
    xzdec \
    gnupg \
    pandoc

RUN tlmgr init-usertree
RUN tlmgr update --all
RUN tlmgr install l3kernel
RUN tlmgr install l3packages
RUN tlmgr install fontspec
RUN tlmgr install float
RUN tlmgr install graphics
RUN tlmgr install tools

RUN mkdir /resources
COPY ingef_logo.jpg /resources/

ENTRYPOINT [ "xelatex" ]

EXPOSE 8000