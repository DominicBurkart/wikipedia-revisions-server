FROM pypy:3

# install revisions downloader
RUN pypy3 -m pip install git+https://github.com/DominicBurkart/wikipedia-revisions.git

# install rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# download & cache crates.io registry and project dependencies
RUN mkdir src && cd src && echo 'fn main() {}' > main.rs
ADD Cargo.toml .
RUN /root/.cargo/bin/cargo fetch

# add code & compile
ADD src/* src/
RUN chmod +x /src/download
RUN /root/.cargo/bin/cargo build
RUN mkfifo revisions.pipe
ENV RUST_BACKTRACE=1
RUN /root/.cargo/bin/cargo test
ENTRYPOINT ["./target/debug/wikipedia-revisions-server"]

# example use:
# docker build -t wikipedia-revisions-server .
# docker run -v /Volumes/doggo:/working_dir -v /Volumes/burkart-6tb/wiki_revisions:/storage_dir wikipedia-revisions-server -d 20200601