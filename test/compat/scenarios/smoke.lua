-- Layer 1: a few smoke scenarios that exercise the harness itself.
return {
    {
        name = 'set_get',
        steps = {
            'set {k}a 5 0 3\r\nabc\r\n',
            'get {k}a {k}missing\r\n',
            'gets {k}a\r\n',
            'cas {k}a 0 0 1 {cas1}\r\nx\r\n',
            'cas {k}a 0 0 1 {cas1}\r\ny\r\n',
            'gets {k}a\r\n',
        },
    },
    {
        name = 'meta_basic',
        steps = {
            'ms {k}m 2 T100 F7 c\r\nhi\r\n',
            'mg {k}m s v f t c k Oop\r\n',
            'md {k}m q\r\n',
            'mg {k}m v\r\n',
        },
    },
    {
        name = 'pipeline_split',
        steps = {
            {chunks = {'se', 't {k}p 0 0 3\r', '\nab', 'c\r\nget {k}p\r\n'}},
        },
    },
    {
        name = 'uppercase',
        steps = {
            {send = 'GET {k}nothing\r\n'},
        },
    },
}
