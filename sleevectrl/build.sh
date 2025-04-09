cd ~/projects
docker build -t docker.io/tlg2132/controller-manager -f sleeve-controller-manager/Dockerfile .
docker push docker.io/tlg2132/controller-manager:latest
cd -

