
## API Folder

The `/api` folder contains the Python FastAPI code.And generation of code by calling seed endpoint.

## Infrastructure Folder

The `/infrastructure-aws` folder contains the CDK code to deploy all the infrastructure
(Lambda function and DynamoDB table) to your AWS account. Lambda 


First, install the node modules.

```bash
npm install
```

Then run bootstrap if you never used CDK with your account before.

```bash
cdk bootstrap
```

Before you can deploy you need to build the front-end folder. Go into the `infrastructure-aws` folder and
build the AlphineJs app.

```bash
npm run build
```

Then go back to the `infrastructure-aws` folder and deploy the stack. The site won't work yet because you'll need to get your API endpoint and update the front-end code too (see below).

```bash
cdk deploy
```

## Frontend Folder

The `/todo-site` contains the AlphineJs frontend that interfaces 


