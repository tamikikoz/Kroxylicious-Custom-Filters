FROM quay.io/kroxylicious/proxy:0.24.0
COPY jars/*.jar /opt/kroxylicious/classpath-plugins/custom-filters/
