def join_filters(*filters):
    """Joins multiple filters"""
    return "[in]{}[out]".format("[out];[out]".join(i for i in filters if i))


def filter_deinterlace():
    """Yadif deinterlace"""
    return "yadif=0:-1:0"


def filter_arc(w, h, aspect):
    """Aspect ratio convertor. Specify output size and source aspect ratio (as float)"""
    taspect = float(w) / h
    if abs(taspect - aspect) < 0.01:
        return f"scale={w}:{h}"
    if taspect > aspect:  # pillarbox
        pt = 0
        ph = h
        pw = int(h * aspect)
        pl = int((w - pw) / 2.0)
    else:  # letterbox
        pl = 0
        pw = w
        ph = int(w * (1 / aspect))
        pt = int((h - ph) / 2.0)
    return f"scale={pw}:{ph}[out];[out]pad={w}:{h}:{pl}:{pt}:black"
