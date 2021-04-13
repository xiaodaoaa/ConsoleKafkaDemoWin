// ConsoleKafkaDemo.cpp : 定义控制台应用程序的入口点。
//

#include "stdafx.h"
#include <iostream>
#include "KafkaConsumerClient.h"
#include "KafkaProducerClient.h"

#if 0
int main()
{
	KafkaConsumerClient *KafkaConsumerClient_ = new KafkaConsumerClient("127.0.0.1:9092", "BAYONET_VEHICLEPASS_JSON_TOPIC", "0", 0, RdKafka::Topic::OFFSET_STORED);//OFFSET_BEGINNING,OFFSET_END
	//KafkaConsumerClient *KafkaConsumerClient_ = new KafkaConsumerClient("41.199.236.245:9092", "XSINK_CROSSING_INTERVAL_ALARM", "0", 0, RdKafka::Topic::OFFSET_STORED);//OFFSET_BEGINNING,OFFSET_END
	if (!KafkaConsumerClient_->Init())
	{
		fprintf(stderr, "kafka server initialize error\n");
		return -1;
	}

	KafkaConsumerClient_->Start(1000);

	return 0;
}

#else
int main()
{
	KafkaProducerClient* KafkaprClient_ = new KafkaProducerClient("41.199.236.245:9092", "BAYONET_VEHICLECROSSING_INTERVAL", 0);
	KafkaprClient_->Init();

	char str_msg[102400] = {0};

	while (fgets(str_msg, sizeof(str_msg), stdin))
	{
		size_t len = strlen(str_msg);
		if (str_msg[len - 1] == '\n')
		{
			str_msg[--len] = '\0';
		}

		if (strcmp(str_msg, "end") == 0)
		{
			break;
		}

		KafkaprClient_->Send(str_msg);
	}

	return 0;
}
#endif
