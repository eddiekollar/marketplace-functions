'use strict';          
const AWS = require('aws-sdk');
const S3 = new AWS.S3({});
const Lambda = new AWS.Lambda({});
const IAM = new AWS.IAM({});

const fs = require('fs');
const csv = require('csv');
const _ = require('lodash');
const MongoClient = require('mongodb').MongoClient;

const Mailgun = require('mailgun-js');

module.exports.deployFunction = (event, context, callback) => {
  console.log(event, context);

  const roleParams = {
    RoleName: event.RoleName
  };
  let getRolePromise = IAM.getRole(roleParams).promise();
  let getS3ObjPromise = S3.getObject({
    Bucket: event.Bucket,
    Key: event.S3Key}).promise();

  Promise.all([getRolePromise, getS3ObjPromise]).then(function(results){
    console.log(results);
    const roleData = results[0];
    const s3data = results[1];

    let params = {
      Code: {
        S3Bucket: event.Bucket,
        S3Key: event.S3Key,
        S3ObjectVersion: s3data.VersionId
      },  
      FunctionName: event.FunctionName, 
      Handler: event.Handler, // is of the form of the name of your source file and then name of your function handler
      MemorySize: 128, 
      Publish: true, 
      Role: roleData.Role.Arn, // replace with the actual arn of the execution role you created
      Runtime: event.RunTime, 
      Timeout: 300
    };

    return Lambda.createFunction(params).promise();
  }).then(function(lambdaData){
    console.log(lambdaData);
    const response = {
      success: true,
      data: lambdaData
    };
  callback(null, response);
  }).catch(function(error){
    console.error(error);
    callback(error);
  });
};


const getOrCreateCollection = function(database, collectionName){
  console.log('getOrCreateCollection', collectionName);
  const collection = database.collection(collectionName);
  if(collection) {
    return Promise.resolve(collection);
  }else{
    return database.createCollection(collectionName);
  }
};

module.exports.parseBilling = (event, context, callback) => {
  const mongoURL = process.env.MONGO_URL;

  if(!mongoURL){
    callback(new Error('No MongoURL found'));
  }

  const fields = ["UsageType","Operation","UsageStartDate","UsageEndDate","UsageQuantity","Cost","ResourceId"];
  const patternARN = new RegExp('arn:aws:lambda', 'i');
  const parser = csv.parse({delimiter: ',', columns: true}, function(err, output){

  });

  console.log('parseBilling: setting up');
  parser.on('error', function(err){
    console.error('parser',err.message);
    //callback(err);
  });
  parser.on('finish', function(){
    // console.log('finished');
  });

  console.log('parseBilling: getting S3 data');
  let s3Object = S3.getObject({
      Bucket: 'market-billing-data',
      Key: 'billing.csv'})

  let s3Stream= s3Object.createReadStream();

  let database;

  MongoClient.connect(mongoURL).then(function(db){
    console.log('parseBilling: connecting to MongoDB');
    database = db;
    return getOrCreateCollection(db, 'usagestats');
  }).then(function(collection){
    const transformer = csv.transform(function(data){
      let obj = {};
      if(data.ResourceId && patternARN.test(data.ResourceId)){
        obj = _.pick(data, fields);
        obj = _.mapValues(obj, function(value, key, o){
          if(key.indexOf('Date') > 0) {
            return new Date(value);
          }else{
            return value;
          }
        });
        obj.source = 'AWS';
        collection.insert(obj);
      }
    });

    transformer.on('error', function(err){
      console.error('transformer error', err.message);
      database.close();
      callback(err);
    });

    transformer.on('finish', function(){
      console.log('transformer finished');
      database.close();
      callback(null, {data: 'success!'});
    });

    s3Stream.pipe(parser).pipe(transformer);
  }).catch((error) => {
    console.error('caught error', error);
    callback(error);
  });
};

module.exports.sendEmail = (event, context, callback) => {
  console.log(event);
  const api_key = process.env.EMAIL_API_KEY;
  const domain = process.env.EMAIL_DOMAIN;

  const mailgun = new Mailgun({apiKey: api_key, domain: domain});

  const data = {
    from: event.from,
    to: event.to,
    subject: event.subject,
    text: event.text
  };
  
  mailgun.messages().send(data, function (error, body) {
    if (error) {
      console.log(error);
      callback(error);
    }
    else {
      callback(null, {success: true, message: 'email sent successfully'});
    }
  });
}