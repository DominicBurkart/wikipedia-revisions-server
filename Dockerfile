FROM pypy:3

# install docker (used in distributed mode)
RUN apt-get update
RUN apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
RUN echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get install -y docker-ce docker-ce-cli containerd.io
RUN docker -v

# install kubectl (used in distributed mode)
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl

# install rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly-2020-09-28

# install revisions downloader
RUN pypy3 -m pip install git+https://github.com/DominicBurkart/wikipedia-revisions.git

# download & cache crates.io registry and project dependencies
RUN mkdir src && cd src && echo 'fn main() {}' > main.rs
ADD Cargo.toml .
RUN /root/.cargo/bin/cargo fetch

# make directories
RUN mkdir /pipes
RUN mkdir /big_dir
RUN mkdir /fast_dir

# add test data
ADD test_data/* test_data/

# add code & compile
ADD src/* src/
RUN chmod +x /src/download
RUN RUSTFLAGS='--cfg procmacro2_semver_exempt -Z macro-backtrace' /root/.cargo/bin/cargo test
RUN RUSTFLAGS='--cfg procmacro2_semver_exempt -Z macro-backtrace' /root/.cargo/bin/cargo build --release
RUN mv ./target/release/wikipedia-revisions-server ./wikipedia-revisions-server
RUN rm -r ./target

# set env variables
ENV TERM xterm-256color
ENV RUST_BACKTRACE=1

ENTRYPOINT ["./wikipedia-revisions-server"]

# example use:
# docker build -t wikipedia-revisions-server . && docker run -it -v /Volumes/doggo:/fast_dir -v /Volumes/burkart-6tb/wiki_revisions:/big_dir wikipedia-revisions-server -d 20200601
