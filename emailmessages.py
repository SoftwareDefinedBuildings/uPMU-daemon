senderaddr = ''
receiveraddr = ''

def makeNoSerialMessage(alerttime, peer):
    return """Hello,
    This email is to inform you that a uPMU has not sent any data for {0} seconds.
    No data has been received at all and so the serial number of the unit is unknown.
    
    Here is some information about the device:
    {1}
    
    This is an automated message. You should not reply to it.""".format(alerttime, peer)
    txt = MIMEText(msg)
    del txt['Subject']
    del txt['From']
    del txt['To']
    txt['Subject'] = 'Automated uPMU Notification'
    txt['From'] = senderaddr
    txt['To'] = receiveraddr
    return txt
    
def makeSerialMessage(alerttime, serial, peer):
    return """Hello,
    This email is to inform you that a uPMU has not sent any data for {0} seconds.
    The serial number of the device is {1}.
    
    Here is some additional information about the device:
    {2}
    
    This is an automated message. You should not reply to it.""".format(alerttime, serial, peer)
    txt = MIMEText(msg)
    del txt['Subject']
    del txt['From']
    del txt['To']
    txt['Subject'] = 'Automated uPMU Notification'
    txt['From'] = senderaddr
    txt['To'] = receiveraddr
    return txt
