CC = cc
CFLAGS = -DDEBUG -D_GNU_SOURCE -Wall -Wextra -pthread
TARGET = netsiohub

all: $(TARGET)

$(TARGET): netsiohub.c
	$(CC) $(CFLAGS) -o $(TARGET) netsiohub.c

clean:
	rm -f $(TARGET)