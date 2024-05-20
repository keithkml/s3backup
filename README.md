## How to use

[Install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html#getting-started-install-instructions)

[Configure the AWS SDK](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration)
Run 

```sh
aws configure
```

Which puts stuff in `~/.aws`

```sh
virtualenv --no-site-packages --distribute .venv && source .venv/bin/activate && pip install -r requirements.txt
```

Then

```sh
./backup_dirs.py <bucketname> <path on disk>
```