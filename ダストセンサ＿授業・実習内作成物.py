# -*- coding: utf-8 -*-


from gpiozero import MCP3208,LED
from time import sleep
import RPi.GPIO

led =LED(25)
MOTOR_IN1=22
MOTOR_IN2=23

GPIO.setmode(GPIO.BCM)
GPIO.setup(MOTOR_IN1, GPIO.OUT)
GPIO.setup(MOTOR_IN2, GPIO.OUT)

abc0=MCP3208(0)

def motor_on():
    GPIO.output(MOTOR_IN1, GPIO.HIGH)
    GPIO.output(MOTOR_IN2, GPIO.LOW)
def motor_off():
    GPIO.output(MOTOR_IN1, GPIO.LOW)
    GPIO.output(MOTOR_IN2, GPIO.LOW)

last_stop_time = time()

try:
    while.True:
        now=time()
        if now -last_stop_time >=30:
            motor_off()
            led.off()
            sleep(60)
            last_stop_time =time()
        value = abc0.value
        voltage =value *3.3
        concentration = 170 *voltage -100
        if concentration <0:
            concentration =0
        if concentration >100:
            led.on()
            motor_on()
        else:
            led.off()
            motor_off()
        sleep(1)
except keyboardInterrupt:
    pass

finally:
    abc0.close()
    motor_off()
    GPIO.output(LED_PIN,GPIO.LOW)
    GPIO.cleanup()