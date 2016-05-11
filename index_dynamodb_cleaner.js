// If this file contains double-curly-braces, that's because
// it is a template that has not been processed into JavaScript yet.
console.log('Loading event');
exports.handler = function(event, context) {{
  var AWS = require('aws-sdk');
  var sns = new AWS.SNS();
  var ml = new AWS.MachineLearning();
  var docclient = new AWS.DynamoDB();
  var endpointUrl = '';
  var mlModelId = 'cleanermodel1';
  var snsTopicArn = '';
  var numMessagesProcessed = 0;
  var numMessagesToBeProcessed = event.Records.length;
  console.log("numMessagesToBeProcessed:"+numMessagesToBeProcessed);
  var second = new Date().getTime() / 1000;
  var currenttime = Math.floor(((second-14400) % 86400)/60);
  var text="";
  var outputtext="";
  var nowid="";
  var boxnum="";

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

//Call AWS Machine Learning Model for Prediction
  var callPredict = function(tweetData){{
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
          console.log('data'+JSON.stringify(data));
          //if there is leak
          if (data.Prediction.predictedLabel==='1') {{
            updateSns("clean box"+boxnum);
          }}
          //no leak
          else {{
            console.log( 'No need for clean');
            context.done(null, 'End clean process');
          }}
        }}
      }}
      );
  }}

//process recodes form Kinesis
  var processRecords = function(){{
    for(i = 0; i < numMessagesToBeProcessed; ++i) {{
      if (event.Records[i].eventName=="INSERT") {{
        encodedPayload = event.Records[i].dynamodb.NewImage;
        // Amazon Kinesis data is base64 encoded so decode here
        console.log("payload:"+JSON.stringify(encodedPayload));
        try {{
          //extract feature for prediction cleaning
          nowid=encodedPayload.ID.S;
          var headerID=nowid.split("_")
          console.log("ID "+nowid);
          console.log("ID "+headerID);
          var parsedPayload = {};
          boxnum=headerID[0];
          parsedPayload['t1']=encodedPayload.t1.S
          parsedPayload['t2']=encodedPayload.t2.S
          parsedPayload['t3']=encodedPayload.t3.S
          parsedPayload['t4']=encodedPayload.t4.S
          parsedPayload['t5']=encodedPayload.t5.S
          parsedPayload['t6']=encodedPayload.t6.S
          parsedPayload['t7']=encodedPayload.t7.S
          parsedPayload['t8']=encodedPayload.t8.S
          parsedPayload['t9']=encodedPayload.t9.S
          parsedPayload['t10']=encodedPayload.t10.S
          parsedPayload['t11']=encodedPayload.t11.S
          parsedPayload['t12']=encodedPayload.t12.S
          parsedPayload['t13']=encodedPayload.t13.S
          parsedPayload['t14']=encodedPayload.t14.S
          parsedPayload['t15']=encodedPayload.t15.S
          parsedPayload['t16']=encodedPayload.t16.S
          parsedPayload['t17']=encodedPayload.t17.S
          parsedPayload['t18']=encodedPayload.t18.S
          parsedPayload['t19']=encodedPayload.t19.S
          parsedPayload['t20']=encodedPayload.t20.S
          parsedPayload['t21']=encodedPayload.t21.S
          parsedPayload['t22']=encodedPayload.t22.S
          parsedPayload['t23']=encodedPayload.t23.S
          parsedPayload['t24']=encodedPayload.t24.S
          parsedPayload['t25']=encodedPayload.t25.S
          parsedPayload['t26']=encodedPayload.t26.S
          parsedPayload['t27']=encodedPayload.t27.S
          parsedPayload['t28']=encodedPayload.t28.S
          parsedPayload['t29']=encodedPayload.t29.S
          parsedPayload['t30']=encodedPayload.t30.S
          parsedPayload['t31']=encodedPayload.t31.S
          parsedPayload['t32']=encodedPayload.t32.S
          parsedPayload['t33']=encodedPayload.t33.S
          parsedPayload['t34']=encodedPayload.t34.S
          parsedPayload['t35']=encodedPayload.t35.S
          parsedPayload['t36']=encodedPayload.t36.S
          parsedPayload['t37']=encodedPayload.t37.S
          parsedPayload['t38']=encodedPayload.t38.S
          parsedPayload['t39']=encodedPayload.t39.S
          parsedPayload['t40']=encodedPayload.t40.S
          parsedPayload['t41']=encodedPayload.t41.S
          parsedPayload['t42']=encodedPayload.t42.S
          parsedPayload['t43']=encodedPayload.t43.S
          parsedPayload['t44']=encodedPayload.t44.S
          parsedPayload['t45']=encodedPayload.t45.S
          parsedPayload['t46']=encodedPayload.t46.S
          parsedPayload['t47']=encodedPayload.t47.S
          parsedPayload['t48']=encodedPayload.t48.S
          parsedPayload['t49']=encodedPayload.t49.S
          parsedPayload['t50']=encodedPayload.t50.S
          parsedPayload['t51']=encodedPayload.t51.S
          parsedPayload['t52']=encodedPayload.t52.S
          parsedPayload['t53']=encodedPayload.t53.S
          parsedPayload['t54']=encodedPayload.t54.S
          parsedPayload['t55']=encodedPayload.t55.S
          parsedPayload['t56']=encodedPayload.t56.S
          parsedPayload['t57']=encodedPayload.t57.S
          parsedPayload['t58']=encodedPayload.t58.S
          parsedPayload['t59']=encodedPayload.t59.S
          parsedPayload['t60']=encodedPayload.t60.S
          parsedPayload['t61']=encodedPayload.t61.S
          parsedPayload['t62']=encodedPayload.t62.S
          parsedPayload['t63']=encodedPayload.t63.S
          parsedPayload['t64']=encodedPayload.t64.S
          parsedPayload['t65']=encodedPayload.t65.S
          parsedPayload['t66']=encodedPayload.t66.S
          parsedPayload['t67']=encodedPayload.t67.S
          parsedPayload['t68']=encodedPayload.t68.S
          parsedPayload['t69']=encodedPayload.t69.S
          parsedPayload['t70']=encodedPayload.t70.S
          parsedPayload['t71']=encodedPayload.t71.S
          parsedPayload['t72']=encodedPayload.t72.S
          parsedPayload['t73']=encodedPayload.t73.S
          parsedPayload['t74']=encodedPayload.t74.S
          parsedPayload['t75']=encodedPayload.t75.S
          parsedPayload['t76']=encodedPayload.t76.S
          parsedPayload['t77']=encodedPayload.t77.S
          parsedPayload['t78']=encodedPayload.t78.S
          parsedPayload['t79']=encodedPayload.t79.S
          parsedPayload['t80']=encodedPayload.t80.S
          parsedPayload['t81']=encodedPayload.t81.S
          parsedPayload['t82']=encodedPayload.t82.S
          parsedPayload['t83']=encodedPayload.t83.S
          parsedPayload['t84']=encodedPayload.t84.S
          parsedPayload['t85']=encodedPayload.t85.S
          parsedPayload['t86']=encodedPayload.t86.S
          parsedPayload['t87']=encodedPayload.t87.S
          parsedPayload['t88']=encodedPayload.t88.S
          parsedPayload['t89']=encodedPayload.t89.S
          parsedPayload['t90']=encodedPayload.t90.S
          parsedPayload['t91']=encodedPayload.t91.S
          parsedPayload['t92']=encodedPayload.t92.S
          parsedPayload['t93']=encodedPayload.t93.S
          parsedPayload['t94']=encodedPayload.t94.S
          parsedPayload['t95']=encodedPayload.t95.S
          parsedPayload['t96']=encodedPayload.t96.S
          parsedPayload['t97']=encodedPayload.t97.S
          parsedPayload['t98']=encodedPayload.t98.S
          parsedPayload['t99']=encodedPayload.t99.S
          parsedPayload['t100']=encodedPayload.t100.S
          parsedPayload['t101']=encodedPayload.t101.S
          parsedPayload['t102']=encodedPayload.t102.S
          parsedPayload['t103']=encodedPayload.t103.S
          parsedPayload['t104']=encodedPayload.t104.S
          parsedPayload['t105']=encodedPayload.t105.S
          parsedPayload['t106']=encodedPayload.t106.S
          parsedPayload['t107']=encodedPayload.t107.S
          parsedPayload['t108']=encodedPayload.t108.S
          parsedPayload['t109']=encodedPayload.t109.S
          parsedPayload['t110']=encodedPayload.t110.S
          parsedPayload['t111']=encodedPayload.t111.S
          parsedPayload['t112']=encodedPayload.t112.S
          parsedPayload['t113']=encodedPayload.t113.S
          parsedPayload['t114']=encodedPayload.t114.S
          parsedPayload['t115']=encodedPayload.t115.S
          parsedPayload['t116']=encodedPayload.t116.S
          parsedPayload['t117']=encodedPayload.t117.S
          parsedPayload['t118']=encodedPayload.t118.S
          parsedPayload['t119']=encodedPayload.t119.S
          parsedPayload['t120']=encodedPayload.t120.S
          parsedPayload['t121']=encodedPayload.t121.S
          parsedPayload['t122']=encodedPayload.t122.S
          parsedPayload['t123']=encodedPayload.t123.S
          parsedPayload['t124']=encodedPayload.t124.S
          parsedPayload['t125']=encodedPayload.t125.S
          parsedPayload['t126']=encodedPayload.t126.S
          parsedPayload['t127']=encodedPayload.t127.S
          parsedPayload['t128']=encodedPayload.t128.S
          parsedPayload['t129']=encodedPayload.t129.S
          parsedPayload['t130']=encodedPayload.t130.S
          parsedPayload['t131']=encodedPayload.t131.S
          parsedPayload['t132']=encodedPayload.t132.S
          parsedPayload['t133']=encodedPayload.t133.S
          parsedPayload['t134']=encodedPayload.t134.S
          parsedPayload['t135']=encodedPayload.t135.S
          parsedPayload['t136']=encodedPayload.t136.S
          parsedPayload['t137']=encodedPayload.t137.S
          parsedPayload['t138']=encodedPayload.t138.S
          parsedPayload['t139']=encodedPayload.t139.S
          parsedPayload['t140']=encodedPayload.t140.S
          parsedPayload['t141']=encodedPayload.t141.S
          parsedPayload['t142']=encodedPayload.t142.S
          parsedPayload['t143']=encodedPayload.t143.S
          parsedPayload['t144']=encodedPayload.t144.S
          parsedPayload['t145']=encodedPayload.t145.S
          parsedPayload['t146']=encodedPayload.t146.S
          parsedPayload['t147']=encodedPayload.t147.S
          parsedPayload['t148']=encodedPayload.t148.S
          parsedPayload['t149']=encodedPayload.t149.S
          parsedPayload['t150']=encodedPayload.t150.S
          parsedPayload['t151']=encodedPayload.t151.S
          parsedPayload['t152']=encodedPayload.t152.S
          parsedPayload['t153']=encodedPayload.t153.S
          parsedPayload['t154']=encodedPayload.t154.S
          parsedPayload['t155']=encodedPayload.t155.S
          parsedPayload['t156']=encodedPayload.t156.S
          parsedPayload['t157']=encodedPayload.t157.S
          parsedPayload['t158']=encodedPayload.t158.S
          parsedPayload['t159']=encodedPayload.t159.S
          parsedPayload['t160']=encodedPayload.t160.S
          parsedPayload['t161']=encodedPayload.t161.S
          parsedPayload['t162']=encodedPayload.t162.S
          parsedPayload['t163']=encodedPayload.t163.S
          parsedPayload['t164']=encodedPayload.t164.S
          parsedPayload['t165']=encodedPayload.t165.S
          parsedPayload['t166']=encodedPayload.t166.S
          parsedPayload['t167']=encodedPayload.t167.S
          parsedPayload['t168']=encodedPayload.t168.S
          parsedPayload['t169']=encodedPayload.t169.S
          parsedPayload['t170']=encodedPayload.t170.S
          parsedPayload['t171']=encodedPayload.t171.S
          parsedPayload['t172']=encodedPayload.t172.S
          parsedPayload['t173']=encodedPayload.t173.S
          parsedPayload['t174']=encodedPayload.t174.S
          parsedPayload['t175']=encodedPayload.t175.S
          parsedPayload['t176']=encodedPayload.t176.S
          parsedPayload['t177']=encodedPayload.t177.S
          parsedPayload['t178']=encodedPayload.t178.S
          parsedPayload['t179']=encodedPayload.t179.S
          parsedPayload['t180']=encodedPayload.t180.S
          parsedPayload['t181']=encodedPayload.t181.S
          parsedPayload['t182']=encodedPayload.t182.S
          parsedPayload['t183']=encodedPayload.t183.S
          parsedPayload['t184']=encodedPayload.t184.S
          parsedPayload['t185']=encodedPayload.t185.S
          parsedPayload['t186']=encodedPayload.t186.S
          parsedPayload['t187']=encodedPayload.t187.S
          parsedPayload['t188']=encodedPayload.t188.S
          parsedPayload['t189']=encodedPayload.t189.S
          parsedPayload['t190']=encodedPayload.t190.S
          parsedPayload['t191']=encodedPayload.t191.S
          parsedPayload['t192']=encodedPayload.t192.S
          parsedPayload['t193']=encodedPayload.t193.S
          parsedPayload['t194']=encodedPayload.t194.S
          parsedPayload['t195']=encodedPayload.t195.S
          parsedPayload['t196']=encodedPayload.t196.S
          parsedPayload['t197']=encodedPayload.t197.S
          parsedPayload['t198']=encodedPayload.t198.S
          parsedPayload['t199']=encodedPayload.t199.S
          parsedPayload['t200']=encodedPayload.t200.S
          parsedPayload['t201']=encodedPayload.t201.S
          parsedPayload['t202']=encodedPayload.t202.S
          parsedPayload['t203']=encodedPayload.t203.S
          parsedPayload['t204']=encodedPayload.t204.S
          parsedPayload['t205']=encodedPayload.t205.S
          parsedPayload['t206']=encodedPayload.t206.S
          parsedPayload['t207']=encodedPayload.t207.S
          parsedPayload['t208']=encodedPayload.t208.S
          parsedPayload['t209']=encodedPayload.t209.S
          parsedPayload['t210']=encodedPayload.t210.S
          parsedPayload['t211']=encodedPayload.t211.S
          parsedPayload['t212']=encodedPayload.t212.S
          parsedPayload['t213']=encodedPayload.t213.S
          parsedPayload['t214']=encodedPayload.t214.S
          parsedPayload['t215']=encodedPayload.t215.S
          parsedPayload['t216']=encodedPayload.t216.S
          parsedPayload['t217']=encodedPayload.t217.S
          parsedPayload['t218']=encodedPayload.t218.S
          parsedPayload['t219']=encodedPayload.t219.S
          parsedPayload['t220']=encodedPayload.t220.S
          parsedPayload['t221']=encodedPayload.t221.S
          parsedPayload['t222']=encodedPayload.t222.S
          parsedPayload['t223']=encodedPayload.t223.S
          parsedPayload['t224']=encodedPayload.t224.S
          parsedPayload['t225']=encodedPayload.t225.S
          parsedPayload['t226']=encodedPayload.t226.S
          parsedPayload['t227']=encodedPayload.t227.S
          parsedPayload['t228']=encodedPayload.t228.S
          parsedPayload['t229']=encodedPayload.t229.S
          parsedPayload['t230']=encodedPayload.t230.S
          parsedPayload['t231']=encodedPayload.t231.S
          parsedPayload['t232']=encodedPayload.t232.S
          parsedPayload['t233']=encodedPayload.t233.S
          parsedPayload['t234']=encodedPayload.t234.S
          parsedPayload['t235']=encodedPayload.t235.S
          parsedPayload['t236']=encodedPayload.t236.S
          parsedPayload['t237']=encodedPayload.t237.S
          parsedPayload['t238']=encodedPayload.t238.S
          parsedPayload['t239']=encodedPayload.t239.S
          parsedPayload['t240']=encodedPayload.t240.S
          parsedPayload['t241']=encodedPayload.t241.S
          parsedPayload['t242']=encodedPayload.t242.S
          parsedPayload['t243']=encodedPayload.t243.S
          parsedPayload['t244']=encodedPayload.t244.S
          parsedPayload['t245']=encodedPayload.t245.S
          parsedPayload['t246']=encodedPayload.t246.S
          parsedPayload['t247']=encodedPayload.t247.S
          parsedPayload['t248']=encodedPayload.t248.S
          parsedPayload['t249']=encodedPayload.t249.S
          parsedPayload['t250']=encodedPayload.t250.S
          parsedPayload['t251']=encodedPayload.t251.S
          parsedPayload['t252']=encodedPayload.t252.S
          parsedPayload['t253']=encodedPayload.t253.S
          parsedPayload['t254']=encodedPayload.t254.S
          parsedPayload['t255']=encodedPayload.t255.S
          parsedPayload['t256']=encodedPayload.t256.S
          parsedPayload['t257']=encodedPayload.t257.S
          parsedPayload['t258']=encodedPayload.t258.S
          parsedPayload['t259']=encodedPayload.t259.S
          parsedPayload['t260']=encodedPayload.t260.S
          parsedPayload['t261']=encodedPayload.t261.S
          parsedPayload['t262']=encodedPayload.t262.S
          parsedPayload['t263']=encodedPayload.t263.S
          parsedPayload['t264']=encodedPayload.t264.S
          parsedPayload['t265']=encodedPayload.t265.S
          parsedPayload['t266']=encodedPayload.t266.S
          parsedPayload['t267']=encodedPayload.t267.S
          parsedPayload['t268']=encodedPayload.t268.S
          parsedPayload['t269']=encodedPayload.t269.S
          parsedPayload['t270']=encodedPayload.t270.S
          parsedPayload['t271']=encodedPayload.t271.S
          parsedPayload['t272']=encodedPayload.t272.S
          parsedPayload['t273']=encodedPayload.t273.S
          parsedPayload['t274']=encodedPayload.t274.S
          parsedPayload['t275']=encodedPayload.t275.S
          parsedPayload['t276']=encodedPayload.t276.S
          parsedPayload['t277']=encodedPayload.t277.S
          parsedPayload['t278']=encodedPayload.t278.S
          parsedPayload['t279']=encodedPayload.t279.S
          parsedPayload['t280']=encodedPayload.t280.S
          parsedPayload['t281']=encodedPayload.t281.S
          parsedPayload['t282']=encodedPayload.t282.S
          parsedPayload['t283']=encodedPayload.t283.S
          parsedPayload['t284']=encodedPayload.t284.S
          parsedPayload['t285']=encodedPayload.t285.S
          parsedPayload['t286']=encodedPayload.t286.S
          parsedPayload['t287']=encodedPayload.t287.S
          parsedPayload['t288']=encodedPayload.t288.S
          parsedPayload['t289']=encodedPayload.t289.S
          parsedPayload['t290']=encodedPayload.t290.S
          parsedPayload['t291']=encodedPayload.t291.S
          parsedPayload['t292']=encodedPayload.t292.S
          parsedPayload['t293']=encodedPayload.t293.S
          parsedPayload['t294']=encodedPayload.t294.S
          parsedPayload['t295']=encodedPayload.t295.S
          parsedPayload['t296']=encodedPayload.t296.S
          parsedPayload['t297']=encodedPayload.t297.S
          parsedPayload['t298']=encodedPayload.t298.S
          parsedPayload['t299']=encodedPayload.t299.S
          parsedPayload['t300']=encodedPayload.t300.S
          parsedPayload['t301']=encodedPayload.t301.S
          console.log(parsedPayload['t301'].toString());
          console.log(parsedPayload['t101'].toString());
          //context.done(null, "finish records");
          //console.log(parsedPayload["t1"].toString); 
          callPredict(parsedPayload);   
        }}
        catch (err) {{
          console.log(err, err.stack);
          context.done(null, "failed payload"+encodedPayload);
        }}
      }}
      else {{
            context.done(null, "Not Insert");
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