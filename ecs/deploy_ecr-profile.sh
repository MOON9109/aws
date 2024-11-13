

docker build --platform=linux/amd64 -t [container_name] .

aws ecr get-login-password --profile $1 --region $2 | docker login --username AWS --password-stdin $3.dkr.ecr.$2.amazonaws.com/mwaa/ecs
docker tag [container_name]:latest $3.dkr.ecr.$2.amazonaws.com/mwaa/ecs:latest
docker push $3.dkr.ecr.$2.amazonaws.com/mwaa/ecs


