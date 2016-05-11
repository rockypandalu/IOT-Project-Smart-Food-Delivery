// If this file contains double-curly-braces, that's because
// it is a template that has not been processed into JavaScript yet.
console.log('Loading event');
exports.handler = function(event, context) {{
  var AWS = require('aws-sdk');
  var sns = new AWS.SNS();
  var ml = new AWS.MachineLearning();
  var docclient = new AWS.DynamoDB();
  var endpointUrl = '';
  var mlModelId = 'ordertimemodel1';
  var table='order_storage';
  var rate_table='Restaurant';
  var snsTopicArn = '';
  var numMessagesProcessed = 0;
  var numMessagesToBeProcessed = event.Records.length;
  console.log("numMessagesToBeProcessed:"+numMessagesToBeProcessed);
  var delivtime = 0;
  var searchtime = 0;
  var arrivetime = 0;
  var getflag = 0;
  var oldsearch = -2;
  var flag = 0;
  var desiretime = 0;
  var second = new Date().getTime() / 1000;
  var currenttime = Math.floor(((second-14400) % 86400)/60);
  var iteration = 0;
  var text="";
  var outputtext="";
  var nowid="";

//Sending SNS Message
  var updateSns = function(Data) {{
    var params = {};
    params['TopicArn'] = snsTopicArn;
    params['Subject']  = Data.toString();
    params['Message']  = Data.toString();
    console.log('Calling Amazon SNS to publish.');
    sns.publish(
      params,
      function(err, data) {{
        if (err) {{
          console.log(err, err.stack); // an error occurred
          context.done(null, 'Failed when publishing to SNS');
        }}
        else {{
          context.done(null, 'Published to SNS');
        }}
      }}
      );
  }}

//change time in minite to common format
  var changeToTime = function(time) {{
      var temp=Math.floor(time/60);
      outputtext=temp.toString();
      temp=time%60;
      if (temp<10) {{
          outputtext=outputtext+":0"+temp.toString();
      }}
      else {{
          outputtext=outputtext+":"+temp.toString();
      }}
  }}

//save time to place order to restaurant
  var updateTable = function(searchtime, arrivetime) {{
    var params = {
      "TableName":table,
      "Item":{
        "ID":{"S":nowid},
        "RestaurantOrderTime":{"N":searchtime.toString()},
        "ArriveTime":{"N":arrivetime.toString()}
      },
      "ReturnValues":"ALL_OLD"
    };
    docclient.putItem(params, function(err,data) {{
      if (err) {{
        console.log(err);
        context.done(null, 'Put DynamoDB failed.');
      }}
      else {{
        changeToTime(searchtime);
        text="Restaurant Notify Time: "+ outputtext;
        changeToTime(arrivetime);
        text=text+" Food Arrival Time: "+outputtext;
        updateSns(text);
      }}
    }}
    );
  }}
  
  //update the rate to restaurant
  var updateRate = function(rating,restaurantid) {{
      var params = {
          "TableName":rate_table,
          "Key":{
              "ObjectId":{"S":restaurantid.toString()}
          }
      };
      docclient.getItem(params,function(err,data) {{
          if (err) {{
              console.log(err);
              context.done(null, 'Get DynamoDB failed.');
          }}
          else {{
              console.log("data:"+JSON.stringify(data));
              var ratetime=parseInt(data.Item.Ratingtime.N);
              var nowrate=parseFloat(data.Item.Rating.N);
              var newrate=(nowrate*ratetime+rating)/(ratetime+1);
              var newratetime=ratetime+1;
              var params_new = {
                  "TableName":rate_table,
                  "Item":{
                    "ObjectId":{"S":restaurantid.toString()},
                    "Ratingtime":{"N":newratetime.toString()},
                    "Rating":{"N":newrate.toString()},
                    "Distance":{"N":data.Item.Distance.N},
                    "UserName":{"S":data.Item.UserName.S}
                  },
                  "ReturnValues":"ALL_OLD"
                };
              docclient.putItem(params_new, function(err,data) {{
              if (err) {{
                console.log(err);
                context.done(null, 'Put DynamoDB failed.');
              }}
              else {{
                context.done(null, 'Rate updated.');
              }}
            }}
            );
          }}
      }}
      );
  }}

//Call AWS Machine Learning Model for Prediction
//iteration to search for placing order time
  var callPredict = function(tweetData){{
    delivtime=0;
    console.log(searchtime.toString()+" searchtime");
    console.log('calling predict'+iteration.toString());
    console.log('calling predict');
    ml.predict(
      {
        Record : tweetData,
        PredictEndpoint : endpointUrl,
        MLModelId: mlModelId
      },
      function(err, data) {{
        if (err) {{
          console.log(err);
          context.done(null, 'Call to predict service failed.');
        }}
        else {{
          console.log('Predict call succeeded');
          iteration=iteration+1;
          delivtime=parseFloat(data.Prediction.predictedValue);
          arrivetime=Math.ceil(searchtime+delivtime);
          console.log(delivtime.toString()+"in predict");
          //iteration exceed
          if (iteration>5) {{
              flag=1;
              text="Fail";
              updateSns("Fail");
          }}
          //time find
          if (((arrivetime<desiretime)&&(arrivetime>desiretime-5)) || (Math.abs(oldsearch-searchtime)<2)) {{
              flag=1;
              second = new Date().getTime() / 1000;
              currenttime = Math.floor(((second-14400) % 86400)/60);
              if (searchtime<currenttime) {{
                  searchtime=currenttime+1;
                  tweetData['ordertime']=searchtime.toString();
                  ml.predict(
                      {
                          Record : tweetData,
                          PredictEndpoint : endpointUrl,
                          MLModelId: mlModelId
                      },
                      function(err, data) {{
                          if (err) {{
                              console.log(err);
                              context.done(null, 'Call to predict service failed.');
                          }}
                          else {{
                              console.log('Predict call succeeded in current');
                              delivtime=parseFloat(data.Prediction.predictedValue);
                              arrivetime=Math.ceil(searchtime+delivtime);
                              changeToTime(searchtime);
                              text="Restaurant Notify Time: "+ outputtext;
                              changeToTime(arrivetime);
                              text=text+" Food Arrival Time: "+outputtext;
                              updateTable(searchtime, arrivetime);
                          }}
                      }}
                  );
              }}
              //find new time
              else {{
                  changeToTime(searchtime);
                  text="Restaurant Notify Time: "+ outputtext;
                  changeToTime(arrivetime);
                  text=text+" Food Arrival Time: "+outputtext;
                  updateTable(searchtime, arrivetime);
              }}
          }}
          else if (arrivetime>desiretime) {{
              oldsearch=searchtime;
              oldarrive=arrivetime;
              searchtime=searchtime-(arrivetime-desiretime);
          }}
          else {{
            oldsearch=searchtime;
            oldarrive=arrivetime;
            searchtime=searchtime+(desiretime-arrivetime);
          }}
          tweetData['ordertime']=searchtime.toString();
          console.log('calling predict'+iteration.toString());
          if (flag==0) {{
            callPredict(tweetData);
          }}
        }}
      }}
      );
  }}

//process recodes form Kinesis
  var processRecords = function(){{
    for(i = 0; i < numMessagesToBeProcessed; ++i) {{
      console.log("event:"+JSON.stringify(event.Records[i]));
      if (event.Records[i].eventName=="INSERT") {{
        encodedPayload = event.Records[i].dynamodb.NewImage;
        // Amazon Kinesis data is base64 encoded so decode here
        console.log("payload:"+JSON.stringify(encodedPayload));
        try {{
          //extract feature for prediction
          nowid=encodedPayload.ID.S;
          console.log("ID"+nowid);
          var parsedPayload = {};
          parsedPayload['ordertime']=encodedPayload.OrderTime.N;
          parsedPayload['restaurant']=encodedPayload.Restaurant.N;
          parsedPayload['hotfood']=encodedPayload.HotFood.N;
          parsedPayload['coldfood']=encodedPayload.ColdFood.N;
          parsedPayload['drink']=encodedPayload.Drink.N;
          parsedPayload['distance']=encodedPayload.Distance.N;
          //console.log("newload_ordertime:"+newload['ordertime']);
          desiretime=parseInt(parsedPayload['ordertime']);
          if ((desiretime-60)>currenttime) {{
            searchtime=desiretime-60;
          }}
          else {{
            searchtime=currenttime;
          }}
          getflag=0;
          parsedPayload['ordertime']=searchtime.toString();        
          callPredict(parsedPayload);   
        }}
        catch (err) {{
          console.log(err, err.stack);
          context.done(null, "failed payload"+payload);
        }}
      }}
      //extract rate of restaurant
      else if (event.Records[i].eventName=="MODIFY") {{
          encodedPayload = event.Records[i].dynamodb.NewImage;
          var rating=parseFloat(encodedPayload.Rating.N);
          var restaurantid=encodedPayload.Restaurant.N;
          console.log(rating.toString()+"restid: "+restaurantid.toString());
          updateRate(rating,restaurantid);
      }}
      else {{
          context.done(null, "not command");
      }}
    }}
    //context.done(null, 'Records End.');
  }}

//Get Machine Learning Model
  var checkRealtimeEndpoint = function(err, data){{
    if (err){{
      console.log(err);
      context.done(null, 'Failed to fetch endpoint status and url.');
    }}
    else {{
      var endpointInfo = data.EndpointInfo;

      if (endpointInfo.EndpointStatus === 'READY') {{
        endpointUrl = endpointInfo.EndpointUrl;
        console.log('Fetched endpoint url :'+endpointUrl);
        processRecords();
      }} else {{
        console.log('Endpoint status : ' + endpointInfo.EndpointStatus);
        context.done(null, 'End point is not Ready.');
      }}
    }}
  }}
  
  ml.getMLModel({MLModelId:mlModelId}, checkRealtimeEndpoint);
  
}};