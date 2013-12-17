import matplotlib
import io
import Image
from pylab import *

# set engine
matplotlib.use('Agg')

class Spark:
    """
    Generate spark plots.
    """

    @staticmethod
    def plot (data, width=2.2, height=0.32, volume=None, enable_volume=False):
        """
        Plot a spark line and a volume density colour bar.
        """
        close() # clear any existing plots #TODO make this thread safe
        num_points = len(data)
        min_data = data.min()
        max_data = data.max()
        min_index = data.argmin()
        max_index = data.argmax()

        fig = figure(1, figsize=(width, height), dpi=600)
        line_colour = "gray"

        if enable_volume != None:
            imshow(np.matrix(volume), norm=matplotlib.colors.LogNorm(), aspect="auto", extent=[0, num_points, min_data, max_data])
            line_colour = "#2f4f4f"   
 
        plot(range(num_points), data, color=line_colour)
        axis([-1, num_points, min_data, max_data ])
        fig.get_axes()[0].get_xaxis().set_visible(False)
        fig.get_axes()[0].get_yaxis().set_visible(False)
        axis('off')
        return Spark.generate()

    @staticmethod
    def generate ():
        buf = io.BytesIO()
        savefig(buf, format="png", transparent=True, pad_inches=0, bbox_inches='tight')
        buf.seek(0)
        return buf

