import configparser
from consumption import main as consume
from replenishment import main as replenish


class ProductionReplenishment:

    def __init__(self, loc):
        input('Start?')
        self.counter = 0
        self.size = 10
        self.loc = loc

    def play(self):
        while self.counter%self.size != 0:
            self.counter = self.counter + 1
            consume(self.loc)
            #input('consuming')
            print(f"-------------------{self.size - self.counter} left until replenishment-------------------")
            self.play()
        if self.counter == 0:
            self.counter = self.counter + 1
            #input('Starting 1/'+str(self.size))
            consume(self.loc)
            print(f"-------------------{self.size - self.counter} left until replenishment-------------------")
            self.play()
        else:
            print('consuming')
            print('replenishing')
            consume(self.loc)
            replenish()
            self.counter = 0
            print(f"-------------------{self.size - self.counter} left until replenishment-------------------")
            self.play()


def main():
    try:
        config = configparser.ConfigParser()
        config.read('prodReplenishment.ini')
        loc = config['DEFAULT']['consumption_loc_id']
        prod_rep = ProductionReplenishment(loc)
        prod_rep.play()
    except Exception as e:
        print(f'An error occurred while running script: {e}',input("Press enter to proceed..."))


if __name__ == "__main__":
    main()
