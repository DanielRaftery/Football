import sqlalchemy as sqla
from sshtunnel import SSHTunnelForwarder


def create_ssh_tunnel():
    print("Opening SSH Tunnel...")
    with open("auth.txt", "r") as file:
        text = file.readlines()

    ec2_url = text[4].strip('\n')
    ec2_port = text[5].strip('\n')
    ec2_user = text[6].strip('\n')
    ec2_key = text[7].strip('\n')
    url = text[0].strip('\n')
    port = text[2].strip('\n')

    server = SSHTunnelForwarder(
        (ec2_url, int(ec2_port)),
        ssh_username=ec2_user,
        ssh_pkey=ec2_key,
        remote_bind_address=(url, int(port)),
        local_bind_address=('', 1111)
    )
    server.start()
    print("Tunnel active!")
    return server


def connect_to_db(schema):
    with open("auth.txt", "r") as file:
        text = file.readlines()
    user = text[1].strip('\n')
    dbname = str(schema)
    password = text[3].strip('\n')

    # server.start()
    eng = "mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, '', 1111, dbname)
    engine = sqla.create_engine(eng, echo=False)
    return engine  # , server


if __name__ == '__main__':
    create_ssh_tunnel()
