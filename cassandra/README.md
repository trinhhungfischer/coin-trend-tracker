# KeySpace [tweets_info](createTweetsTable.cql)
Key Space tweets_info được tạo trong file [createTweetsTable.cql](createTweetsTable.cql) với 2 table 
## recent_tweets
sẽ chứa thông tin của các tweet mới nhất trong vòng 2 phút => Mục tiêu của bảng này là sẽ hiển thị các tweet hot cho người dùng (có thể đơn giản chỉ là một list các link tới tweet). Link tweet có thể đơn giản tạo ra bằng "twitter.com/anyuser/status/" + tweet_id.
## total_tweets_per_hashtag
sẽ chứa thông tin của các tweet mới cập nhật về các thông số về tweet cho từng hashtag => Mục tiêu của bảng sẽ hiển thị được tổng cộng các thông số của 100 hashtag coin bao gồm số tweet, like, replies, ... Có thể cho những phân tích về tổng số lượng tweet tới lúc record_time cho mỗi hashtag, trending cho tới thời gian đó
## các bảng sau
Chưa làm tới nên tao cx chưa nghĩ ra nên bạn cứ làm 2 cái kia trước nha. Làm được cái total_tweets_per_hastag thì mấy cái sau cũng chỉ là nhân bản lên thôi.