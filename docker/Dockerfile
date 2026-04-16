FROM debian:bullseye

RUN apt-get update && apt-get install -y \
    texlive-latex-base \
    texlive-latex-extra \
    texlive-lang-european \
    texlive-bibtex-extra \
    biber \
    git \
    texlive-xetex \
    python3-pip \
    latexmk \
    python3 \
    python3-pygments \
    python-is-python3 \
    && pip install pygments \
    && rm -rf /var/lib/apt/lists/*
COPY fonts/ /usr/share/fonts/opentype
RUN fc-cache -f -v
RUN mkdir -p /bp/output && chmod -R 777 /bp
