public interface IKafkaConstants {
    public static String KAFKA_BROKERS = "ec2-35-165-113-215.us-west-2.compute.amazonaws.com:9092," +
            "ec2-54-69-173-183.us-west-2.compute.amazonaws.com:9092," +
            "ec2-54-218-166-98.us-west-2.compute.amazonaws.com:9092," +
            "ec2-52-24-63-41.us-west-2.compute.amazonaws.com:9092";
    public static Integer MESSAGE_COUNT=1000;
    public static String CLIENT_ID="client1";
    public static String TOPIC_NAME="test_data";
    public static String GROUP_ID_CONFIG="consumerGroup1";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
    public static String OFFSET_RESET_LATEST="latest";
    public static String OFFSET_RESET_EARLIER="earliest";
    public static Integer MAX_POLL_RECORDS=1;
}