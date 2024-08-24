# Use a base image with Go installed
FROM golang:1.23

# Set the working directory
WORKDIR /app

# Copy the source code into the container
COPY . .

# Build the application
RUN go build -o server ./app/

# Expose the necessary ports (adjust as needed)
EXPOSE 6379 6380

# Command to run the application
CMD ["./server"]
