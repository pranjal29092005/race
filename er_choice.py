import click


class Choice(object):
    def __init__(self, **kwargs):
        self.min_val = kwargs.get('min', 1)
        self.max_val = kwargs.get('max', 10)
        self.prompt = kwargs.get(
            'prompt', f'Enter Choice ({self.min_val}-{self.max_val})')

    def _check(self, abc):
        rng = range(self.min_val, self.max_val + 1)
        try:
            if int(abc) in rng:
                return abc
            else:
                return None
        except ValueError:
            pass

        if ',' in abc:
            vals = []
            for val in map(int, abc.split(',')):
                if val in rng:
                    vals.append(val)
            return vals
        elif '-' in abc:
            vals = []
            min, max = abc.split('-')
            sub_rng = range(int(min), int(max) + 1)
            for v in sub_rng:
                if v in rng:
                    vals.append(v)
            return vals
        elif abc == 'all':
            return list(rng)
        else:
            return None

    def get_choice(self):
        choice = click.prompt(self.prompt, value_proc=self._check)
        return choice
