FROM openjdk:latest
 
ADD ./CAPOC-0.0.1-SNAPSHOT-shaded.jar app/

WORKDIR /app

EXPOSE 8089

CMD ["java", "-cp", "./CAPOC-0.0.1-SNAPSHOT-shaded.jar", "com.tryg.poc.KstreamMain"]  

 
