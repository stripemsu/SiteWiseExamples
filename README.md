# SiteWiseExamples

SiteWiseCreateAndSend.py script requires boto3 linrary installed.
You also need to have you aws keys available in console to run it.

Script:
1. Creates new AWS SiteWise model with measurement, metrics and transform in us-east-1 region
2. Send test data
3. Wait for 10 minutes and perioduically pull data from all properties ot this model.

Script does not clean up data. It should be done manually from AWS Console.
